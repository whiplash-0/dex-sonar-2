import asyncio
import inspect
import logging
from datetime import timedelta
from typing import Callable, Iterable
from requests import exceptions as requests_exceptions
from pybit import unified_trading

from src.core.async_tasks import AsyncConcurrentPollingTasks
from src.pairs.pair import Pair, Symbol, TimeSeries
from src.pairs.pairs import Pairs
from src.pairs.pybit_converters import Contract, Convert, InstrumentInfo, Response, Status
from src.utils import time
from src.utils.time import Cooldowns


CATEGORY = 'linear'


logger = logging.getLogger(__name__)


class WebsocketConnectionLostError(ConnectionError):
    ...


class LivePairs(Pairs):
    def __init__(
            self,
            update_frequency_price: timedelta = timedelta(seconds=5),
            polling_interval_update_instruments_info: timedelta = timedelta(seconds=60),
            polling_interval_monitor_websocket_liveness: timedelta = timedelta(seconds=10),
            polling_interval_distribute_update_cooldowns_uniformly: timedelta = timedelta(seconds=30),
            callback_on_price_update: Callable[[Pair], None] = lambda _: None,
            pairs_filter: Callable[[list[Pair]], Iterable[Pair]] = lambda _: _,
    ):
        super().__init__()
        self.update_frequency_price = update_frequency_price
        self.callback_on_price_update = callback_on_price_update
        self.pairs_filter = pairs_filter

        self.requests = unified_trading.HTTP(testnet=False)
        self.websocket = unified_trading.WebSocket(testnet=False, channel_type=CATEGORY)
        self.permanent_tasks = AsyncConcurrentPollingTasks(
            (self._polling_task_monitor_websocket_liveness, polling_interval_monitor_websocket_liveness),
            (self._polling_task_distribute_update_cooldowns_uniformly, polling_interval_distribute_update_cooldowns_uniformly),
            (self._polling_task_update_instruments_info, polling_interval_update_instruments_info),
        )
        self.price_updates_cooldowns: Cooldowns[Symbol] = Cooldowns(cooldown=update_frequency_price)
        self.are_websocket_callbacks_enabled = False

    async def init(self):
        pairs = Pairs()
        tickers = Convert.get_tickers(self.requests.get_tickers(category=CATEGORY))
        instruments_info = await self._get_instruments_info()

        for ticker in tickers:
            if not ticker.is_prelisted:
                pairs.update(Pair(
                    symbol=ticker.symbol,
                    prices=TimeSeries(step=timedelta(minutes=1)),
                    turnovers=TimeSeries(step=timedelta(minutes=1)),
                    turnover=ticker.turnover,
                    open_interest=ticker.open_interest,
                    funding_rate=ticker.funding_rate,
                    funding_interval=instruments_info[ticker.symbol].funding_interval,
                    next_funding_time=ticker.next_funding_time,
                    delisting_time=instruments_info[ticker.symbol].delisting_time,
                ))

        self.update(self.pairs_filter(pairs))
        self._update_candles()

    async def start_live_updates(self):
        self.websocket.ticker_stream(self.get_symbols(), self._callback_on_ticker_update)
        self.websocket.kline_stream(1, self.get_symbols(), self._callback_on_kline_update)
        self._enable_websocket_callbacks()
        await self.permanent_tasks.run(blocking=True)  # to be able to propagate exceptions

    async def stop_live_updates(self):
        """
        Cancels request tasks and disables callbacks, but does not terminate pybit's websocket thread.

        This limitation is due to a known Pybit bug with the `exit()` method, which may be resolved in future versions.

        Currently, the only reliable way to fully stop the Pybit thread is to terminate the entire program.
        For an immediate exit with no delay or cleanup, use `os._exit(0)`.
        """
        self._disable_websocket_callbacks()
        await self.permanent_tasks.cancel_all()

    def are_live_updates_active(self):
        return self.websocket.is_connected() and self._are_websocket_callbacks_enabled() and not self.permanent_tasks.are_cancelled()

    def _enable_websocket_callbacks(self):
        self.are_websocket_callbacks_enabled = True

    def _disable_websocket_callbacks(self):
        self.are_websocket_callbacks_enabled = False

    def _are_websocket_callbacks_enabled(self):
        return self.are_websocket_callbacks_enabled

    def _callback_on_ticker_update(self, response: Response):
        try:
            if not self._are_websocket_callbacks_enabled(): return

            symbol = response['data']['symbol']

            if not self.price_updates_cooldowns.is_in_cooldown(symbol):
                self.price_updates_cooldowns.set_for(symbol)

                ticker = Convert.stream_ticker(response)
                pair = self[symbol]

                pair.prices.update(
                    ticker.price,
                    time.ceil_timestamp_minute(ticker.timestamp),
                )
                pair.turnover = ticker.turnover
                pair.open_interest = ticker.open_interest
                pair.funding_rate = ticker.funding_rate
                pair.next_funding_time = ticker.next_funding_time

                self.callback_on_price_update(pair)

        except Exception:
            logger.exception(f'Callback `{inspect.currentframe().f_code.co_name}` caught exception'); raise

    def _callback_on_kline_update(self, response: Response):
        """
        Updates pairs' prices and turnovers every minute when the current candlestick is closed
        """
        try:
            if not self._are_websocket_callbacks_enabled(): return

            if response['data'][0]['confirm']:  # if candlestick is final
                kline = Convert.stream_kline(response)
                pair = self[kline.symbol]

                pair.prices.update(
                    kline.close,
                    time.ceil_timestamp_minute(kline.end),
                    is_final=True,
                )
                pair.turnovers.update(
                    kline.turnover,
                    time.ceil_timestamp_minute(kline.end),
                    is_final=True,
                )

        except Exception:
            logger.exception(f'Callback `{inspect.currentframe().f_code.co_name}` caught exception'); raise

    async def _polling_task_monitor_websocket_liveness(self):
        if not self.websocket.is_connected(): raise WebsocketConnectionLostError()

    async def _polling_task_distribute_update_cooldowns_uniformly(self):
        self._disable_websocket_callbacks()

        timestamp = time.get_timestamp()
        delta = self.update_frequency_price / (len(self) - 1) if len(self) > 1 else timedelta(0)
        for i, x in enumerate(self.pairs): self.price_updates_cooldowns.set_start_for(x, timestamp + delta * i - self.price_updates_cooldowns.get_cooldown())

        self._enable_websocket_callbacks()

    async def _polling_task_update_instruments_info(self):
        instruments_info = await self._get_instruments_info()
        for symbol, pair in self.pairs.items(): pair.funding_interval = instruments_info[symbol].funding_interval

    async def _get_instruments_info(self, trials_on_error=3, error_cooldown=timedelta(seconds=1)) -> dict[Symbol, InstrumentInfo]:
        instruments_info = None

        for i in range(1 + trials_on_error):
            try:
                instruments_info = self.requests.get_instruments_info(
                    category=CATEGORY,
                    limit=1000,
                )
                break
            except requests_exceptions.ConnectionError as e:
                logger.warning(
                    f'{inspect.currentframe().f_code.co_name}(): Got {e}' +
                    (f'. Retrying in {error_cooldown.total_seconds()}s' if i < trials_on_error else '')
                )
                if i == trials_on_error: raise
                await asyncio.sleep(error_cooldown.total_seconds())

        return {x.symbol: x for x in Convert.get_instruments_info(instruments_info)}

    def _update_candles(self):
        for symbol in self.get_symbols(): self._update_candle(symbol)

    def _update_candle(self, symbol: Symbol):
        pair = self[symbol]
        kline = Convert.get_kline(
            self.requests.get_kline(
                category=CATEGORY,
                symbol=symbol,
                intervalTime='1',
                limit=1000,
            ),
            from_past_to_present=True,
        )

        # confirmed (closed) candles
        pair.prices.update(
            kline.closes[:-1],
            kline.starts[1:],
            is_final=True,
        )
        pair.turnovers.update(
            kline.turnovers[:-1],
            kline.starts[1:],
            is_final=True,
        )

        # last unconfirmed (unknown status) candle
        pair.prices.update(
            kline.closes[-1],
            kline.starts[-1] + pair.prices.get_time_step(),
        )
        pair.turnovers.update(
            kline.turnovers[-1],
            kline.starts[-1] + pair.turnovers.get_time_step(),
        )
