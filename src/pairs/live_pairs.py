import asyncio
import inspect
import logging
import time as pytime
from datetime import timedelta
from typing import Callable, Iterable

from pybit import unified_trading

from src.core.async_tasks import AsyncTasks
from src.pairs.pair import Pair, Symbol, TimeSeries
from src.pairs.pairs import Pairs
from src.pairs.pybit_converters import Convert, InstrumentInfo, Response
from src.utils import time
from src.utils.time import Cooldowns


CATEGORY = 'linear'


logger = logging.getLogger(__name__)


class LivePairs(Pairs):
    def __init__(
            self,
            update_frequency_price: timedelta,
            update_frequency_instruments_info: timedelta,
            callback_on_price_update: Callable[[Pair], None] = lambda _: None,
            pairs_filter: Callable[[list[Pair]], Iterable[Pair]] = lambda _: _,
    ):
        super().__init__()
        self.callback_on_price_update = callback_on_price_update
        self.pairs_filter = pairs_filter

        self.requests = unified_trading.HTTP(testnet=False)
        self.websocket = unified_trading.WebSocket(testnet=False, channel_type=CATEGORY)
        self.updating_tasks = AsyncTasks(self._task_update_instruments_info(poll_interval=update_frequency_instruments_info))
        self.price_updates_cooldowns: Cooldowns[Symbol] = Cooldowns(cooldown=update_frequency_price)

        self.are_websocket_callbacks_enabled = False
        self._init()

    async def start_continuous_updating(self):
        self._update_candles()
        self.websocket.ticker_stream(self.get_symbols(), self._callback_on_ticker_update)
        self.websocket.kline_stream(1, self.get_symbols(), self._callback_on_kline_update)
        self._enable_websocket_callbacks()
        await self.updating_tasks.run(blocking=True)

    async def stop_continuous_updating(self):
        """
        Cancels request tasks and disables callbacks, but does not terminate pybit's websocket thread.

        This limitation is due to a known Pybit bug with the `exit()` method, which may be resolved in future versions.

        Currently, the only reliable way to fully stop the Pybit thread is to terminate the entire program.
        For an immediate exit with no delay or cleanup, use `os._exit(0)`.
        """
        self._disable_websocket_callbacks()
        await self.updating_tasks.cancel_all()

    def is_updating_active(self):
        return self.websocket.is_connected() and self._are_websocket_callbacks_enabled() and not self.updating_tasks.are_cancelled()

    def _init(self):
        pairs = Pairs()
        tickers = Convert.get_tickers(self.requests.get_tickers(category=CATEGORY))
        instruments_info = self._get_instruments_info()

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
                ))

        self.update(self.pairs_filter(pairs))

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
                self.price_updates_cooldowns.set_cooldown(symbol)

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

    async def _task_update_instruments_info(self, poll_interval: timedelta):
        try:
            while True:
                start = pytime.monotonic()
                instruments_info = self._get_instruments_info()

                for symbol, pair in self.pairs.items():
                    pair.funding_interval = instruments_info[symbol].funding_interval

                await asyncio.sleep(max(poll_interval.total_seconds() - (pytime.monotonic() - start), 0))

        except asyncio.CancelledError:
            logger.debug(f'Task `{inspect.currentframe().f_code.co_name}` was cancelled'); raise

    def _get_instruments_info(self) -> dict[Symbol, InstrumentInfo]:
        return {x.symbol: x for x in Convert.get_instruments_info(self.requests.get_instruments_info(
            category=CATEGORY,
            limit=1000,
        ))}

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
