import inspect
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Iterable, Optional

from src.core.async_tasks import AsyncConcurrentPollingTasks
from src.pairs.pair import Pair, Symbol, TimeSeries
from src.pairs.pairs import Pairs
from src.pairs.pybit_wrapper import CONFIRM, DATA, PybitWrapper, Response, SYMBOL
from src.utils import time
from src.utils.time import Cooldowns


RETRIES_ON_ERROR = 3
RETRY_COOLDOWN = timedelta(seconds=1)


logger = logging.getLogger(__name__)



class ConnectionLostError(ConnectionError):
    ...


@dataclass
class Intervals:
    price_update:                     timedelta = timedelta(seconds=5)
    price_update_staggering:          timedelta = timedelta(seconds=30)
    instruments_info_update:          timedelta = timedelta(seconds=60)
    pairs_list_synchronization:       timedelta = timedelta(seconds=60)
    connection_check:                 timedelta = timedelta(seconds=1)



class LivePairs(Pairs):
    def __init__(
            self,
            intervals: Intervals,
            callback_on_price_update: Callable[[Pair], None] = lambda _: None,
            should_pair_be_included: Callable[[Pair], bool] = lambda _: True,
    ):
        super().__init__()

        self.pybit = PybitWrapper(
            retries_on_error=RETRIES_ON_ERROR,
            retry_cooldown=RETRY_COOLDOWN,
        )
        self.permanent_tasks = AsyncConcurrentPollingTasks(
            (self._polling_task_update_instruments_info, intervals.instruments_info_update),
            (self._polling_task_check_connection,        intervals.connection_check),
            (self._polling_task_stagger_price_updates,   intervals.price_update_staggering),
            (self._polling_task_synchronize_pairs_list,  intervals.pairs_list_synchronization),
        )
        self.ticker_updates_cooldowns: Cooldowns[Symbol] = Cooldowns(
            cooldown=intervals.price_update,
        )

        self.callback_on_price_update = callback_on_price_update
        self.should_pair_be_included = should_pair_be_included
        self.are_pybit_callbacks_enabled = False
        self.cached_instruments_info_symbols: set[Symbol] = set()

    async def init(self):
        await self._add_new_pairs_if_any()


    def are_live_updates_active(self):
        return self.pybit.is_connection_alive() and self._are_pybit_callbacks_enabled() and not self.permanent_tasks.are_cancelled()

    async def start_live_updates(self):
        self._enable_pybit_callbacks()
        self._subscribe_to_live_updates()
        await self.permanent_tasks.run(blocking=True)  # to be able to propagate exceptions

    async def stop_live_updates(self):
        """
        Cancels request tasks and disables callbacks, but does not terminate pybit's websocket thread.

        This limitation is due to a known Pybit bug with the `exit()` method, which may be resolved in future versions.

        Currently, the only reliable way to fully stop the Pybit thread is to terminate the entire program.
        For an immediate exit with no delay or cleanup, use `os._exit(0)`.
        """
        self._disable_pybit_callbacks()
        await self.permanent_tasks.cancel_all()


    async def _add_new_pairs_if_any(self) -> Pairs:
        pairs = Pairs()
        instruments_info = await self.pybit.get_instruments_info()

        if new_symbols := instruments_info.keys() - self.get_symbols():
            tickers = self.pybit.get_tickers()

            for symbol in new_symbols:
                ii = instruments_info[symbol]
                t = tickers[symbol]

                pair = Pair(
                    symbol=t.symbol,

                    prices=TimeSeries(step=timedelta(minutes=1)),
                    turnovers=TimeSeries(step=timedelta(minutes=1)),

                    turnover=t.turnover,
                    open_interest=t.open_interest,
                    funding_rate=t.funding_rate,
                    funding_interval=ii.funding_interval,
                    next_funding_time=t.next_funding_time,
                    delisting_time=ii.delisting_time,
                )

                if self.should_pair_be_included(pair):
                    pairs.update(pair)

            self.update(pairs)
            self._update_candles(pairs.get_symbols())

        return pairs

    def _subscribe_to_live_updates(self, symbols: Optional[Iterable[Symbol]] = None):
        if symbols := symbols if symbols is not None else self.get_symbols():
            self.pybit.subscribe_to_ticker_updates(symbols, self._pybit_callback_on_ticker_update)
            self.pybit.subscribe_to_kline_updates(symbols, self._pybit_callback_on_kline_update)


    async def _polling_task_check_connection(self):
        if not self.pybit.is_connection_alive(): raise ConnectionLostError()

    async def _polling_task_stagger_price_updates(self):
        self._disable_pybit_callbacks()

        timestamp = time.get_timestamp()
        delta = self.ticker_updates_cooldowns.get_cooldown() / (len(self) - 1) if len(self) > 1 else timedelta(0)
        for i, x in enumerate(self.pairs): self.ticker_updates_cooldowns.set_start_for(x, timestamp + delta * i - self.ticker_updates_cooldowns.get_cooldown())

        self._enable_pybit_callbacks()

    async def _polling_task_update_instruments_info(self):
        instruments_info = await self.pybit.get_instruments_info()
        for symbol, pair in self.pairs.items():
            pair.funding_interval = instruments_info[symbol].funding_interval

    async def _polling_task_synchronize_pairs_list(self):
        instruments_info_symbols = (await self.pybit.get_cached_instruments_info()).keys()  # to avoid waiting

        if instruments_info_symbols != self.cached_instruments_info_symbols:  # to also avoid extra waiting
            self.cached_instruments_info_symbols = set(instruments_info_symbols)

            if pairs := await self._add_new_pairs_if_any():
                self._subscribe_to_live_updates(pairs.get_symbols())
                logger.info(f'Added new pairs: {", ".join([x.base_symbol for x in pairs])}')

            if removed_symbols := self.get_symbols() - instruments_info_symbols:
                logger.info(f'Removing pairs: {", ".join([x.base_symbol for x in self if x.symbol in removed_symbols])}')
                self.remove(removed_symbols)


    def _are_pybit_callbacks_enabled(self):
        return self.are_pybit_callbacks_enabled

    def _enable_pybit_callbacks(self):
        self.are_pybit_callbacks_enabled = True

    def _disable_pybit_callbacks(self):
        self.are_pybit_callbacks_enabled = False

    def _pybit_callback_on_ticker_update(self, response: Response):
        try:
            if not self._are_pybit_callbacks_enabled():
                return

            symbol = response[DATA][SYMBOL]

            if not self.ticker_updates_cooldowns.is_in_cooldown(symbol):
                self.ticker_updates_cooldowns.set_for(symbol)

                ticker = self.pybit.parse_stream_ticker(response)
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

    def _pybit_callback_on_kline_update(self, response: Response):
        """
        Updates pairs' prices and turnovers every minute when the current candlestick is closed
        """
        try:
            if not self._are_pybit_callbacks_enabled():
                return

            if response[DATA][0][CONFIRM]:  # if candlestick is final
                kline = self.pybit.parse_stream_kline(response)
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


    def _update_candles(self, symbols: Optional[Iterable[Symbol]] = None):
        symbols = (
            symbols
            if symbols is not None else
            self.get_symbols()
        )
        for x in symbols:
            self._update_pair_candles(x)

    def _update_pair_candles(self, symbol: Symbol):
        pair = self[symbol]
        kline = self.pybit.get_kline(symbol, from_past_to_present=True)

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
