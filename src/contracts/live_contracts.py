import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from src.contracts.contract import Contract, Symbol
from src.contracts.contracts import Contracts
from src.contracts.pybit_wrapper import PybitWrapper, Response
from src.core.workflow_runner import AsyncPollingTasks, ThreadedTasks
from src.support import time_series
from src.utils.time import Cooldowns, Time, Timedelta



logger = logging.getLogger(__name__)


INSTRUMENTS_INFO_RETRIES_ON_ERROR = 3
INSTRUMENTS_INFO_RETRY_COOLDOWN = Timedelta(seconds=1)

CONNECTION_CHECK_RETRIES_ON_FAIL = 3
CONNECTION_CHECK_RETRY_COOLDOWN = Timedelta(seconds=1)



class ConnectionLostError(ConnectionError):
    ...



@dataclass
class Intervals:
    price_update:                          Timedelta = Timedelta(seconds=5)
    price_update_staggering:               Timedelta = Timedelta(seconds=30)
    instruments_info_update:               Timedelta = Timedelta(seconds=60)
    contracts_synchronization_with_server: Timedelta = Timedelta(seconds=60)
    connection_check:                      Timedelta = Timedelta(seconds=1)



class LiveContracts(Contracts):
    def __init__(
            self,
            intervals: Intervals,
            callback_on_price_update: Callable[[Contract], None] = lambda _: None,
            **kwargs,
    ):
        super().__init__(**kwargs)

        self.pybit = PybitWrapper(
            retries_on_error=INSTRUMENTS_INFO_RETRIES_ON_ERROR,
            retry_cooldown=INSTRUMENTS_INFO_RETRY_COOLDOWN,
        )
        self.permanent_tasks = AsyncPollingTasks(
            (
                self._polling_task_update_instruments_info,
                intervals.instruments_info_update,
            ),
            (
                lambda: self._polling_task_check_connection(retries_on_fail=CONNECTION_CHECK_RETRIES_ON_FAIL, retry_cooldown=CONNECTION_CHECK_RETRY_COOLDOWN),
                intervals.connection_check,
            ),
            (
                self._polling_task_stagger_price_updates,
                intervals.price_update_staggering,
            ),
            (
                self._polling_task_synchronize_contracts_with_server,
                intervals.contracts_synchronization_with_server,
            ),
        )
        self.ticker_updates_cooldowns: Cooldowns[Symbol] = Cooldowns(
            cooldown=intervals.price_update,
        )

        self.callback_on_price_update = callback_on_price_update
        self.are_pybit_callbacks_enabled = False
        self.cached_instruments_info_symbols: set[Symbol] = set()

    async def init(self):
        await self._add_new_contracts_if_any()


    async def start_live_updates(self):
        self._enable_pybit_callbacks()
        self._subscribe_to_live_updates()
        await self.permanent_tasks.run()  # to be able to propagate exceptions

    async def stop_live_updates(self):
        """
        Cancels request tasks and disables callbacks, but does not terminate pybit's websocket thread.

        This limitation is due to a known Pybit bug with the `exit()` method, which may be resolved in future versions.

        Currently, the only reliable way to fully stop the Pybit thread is to terminate the entire program.
        For an immediate exit with no delay or cleanup, use `os._exit(0)`.
        """
        self._disable_pybit_callbacks()
        await self.permanent_tasks.stop()


    async def _add_new_contracts_if_any(self) -> Contracts:
        contracts = []
        instruments_info = await self.pybit.fetch_instruments_info(fix_launch_time=True, cached=True)
        tickers = self.pybit.fetch_tickers()

        if new_symbols := instruments_info.keys() & tickers.keys() - self.get_symbols():

            for symbol in new_symbols:
                ii = instruments_info[symbol]
                t = tickers[symbol]
                contracts.append(Contract(
                    symbol=t.symbol,

                    base_symbol=ii.base_symbol,
                    quote_symbol=ii.quote_symbol,

                    launch_time=ii.launch_time,
                    delisting_time=ii.delisting_time,

                    turnover=t.turnover,
                    funding_rate=t.funding_rate,
                    funding_interval=ii.funding_interval,
                    next_funding_time=t.next_funding_time,
                ))

            contracts = self.extend(contracts)
            self._update_candles(contracts.get_symbols())

        else:
            contracts = Contracts()

        return contracts

    def _subscribe_to_live_updates(self, symbols: Optional[Iterable[Symbol]] = None):
        if symbols := symbols if symbols is not None else self.get_symbols():
            self.pybit.subscribe_to_ticker_updates(symbols, self._pybit_callback_on_ticker_update)
            self.pybit.subscribe_to_kline_updates(symbols, self._pybit_callback_on_kline_update)


    async def _polling_task_check_connection(self, retries_on_fail: int = 0, retry_cooldown: Timedelta = Timedelta()):
        for i in range(1 + retries_on_fail):
            if self.pybit.is_connection_alive(): return
            await asyncio.sleep(retry_cooldown.total_seconds())
        raise ConnectionLostError()

    async def _polling_task_stagger_price_updates(self):
        self._disable_pybit_callbacks()

        timestamp = Time.now()
        delta = self.ticker_updates_cooldowns.get_cooldown() / (len(self) - 1) if len(self) > 1 else Timedelta()
        for i, x in enumerate(self): self.ticker_updates_cooldowns.set_start_for(x, timestamp + delta * i - self.ticker_updates_cooldowns.get_cooldown())

        self._enable_pybit_callbacks()

    async def _polling_task_update_instruments_info(self):
        instruments_info = await self.pybit.fetch_instruments_info()

        for symbol in self.get_symbols() & instruments_info.keys():
            contract = self[symbol]
            ii = instruments_info[symbol]

            contract.funding_interval = ii.funding_interval
            contract.delisting_time = ii.delisting_time,

    async def _polling_task_synchronize_contracts_with_server(self):
        instruments_info_symbols = (await self.pybit.fetch_instruments_info(cached=True)).keys()  # to avoid waiting

        if instruments_info_symbols != self.cached_instruments_info_symbols:  # to also avoid extra waiting
            self.cached_instruments_info_symbols = set(instruments_info_symbols)

            if contracts := await self._add_new_contracts_if_any():
                self._subscribe_to_live_updates(contracts.get_symbols())
                logger.info(f'Added contracts: {", ".join(contracts.get_base_symbols())}')

            if removed_symbols := (self.get_symbols() - instruments_info_symbols):
                removed_contracts = self.remove(removed_symbols)
                logger.info(f'Delisted contracts: {", ".join(removed_contracts.get_base_symbols())}')


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

            symbol = self.pybit.extract_symbol(response)

            if not self.ticker_updates_cooldowns.is_in_cooldown(symbol):
                self.ticker_updates_cooldowns.set_for(symbol)

                if contract := self.get(symbol):  # if contract is actually present (there can be mismatch from bybit side)
                    ticker = self.pybit.parse_stream_ticker(response)

                    contract.prices.update(
                        ticker.price,
                        Time.ceil_to_minute(ticker.timestamp),
                    )
                    contract.turnover =          ticker.turnover
                    contract.funding_rate =      ticker.funding_rate
                    contract.next_funding_time = ticker.next_funding_time

                    self.callback_on_price_update(contract)

        except Exception:
            logger.exception(f'`{inspect.currentframe().f_code.co_name}()`: Caught exception:'); raise

    def _pybit_callback_on_kline_update(self, response: Response):
        """
        Updates contracts' prices and turnovers every minute when the current candlestick is closed
        """
        try:
            if not self._are_pybit_callbacks_enabled():
                return

            if self.pybit.is_candle_final(response):
                kline = self.pybit.parse_stream_kline(response)

                if contract := self.get(kline.symbol):  # if contract is actually present (there can be mismatch from bybit side)
                    contract.prices.update(
                        kline.close,
                        Time.ceil_to_minute(kline.end),
                        is_final=True,
                    )
                    contract.turnovers.update(
                        kline.turnover,
                        Time.ceil_to_minute(kline.end),
                        is_final=True,
                    )

        except time_series.InvalidTimeRange:  # fill candle gaps, often happens when websocket connection is temporarily lost
            logger.warning(f'`{inspect.currentframe().f_code.co_name}()`: Detected time gap in candles. Updating all contracts\' candles manually')
            self._update_candles()

        except Exception:
            logger.exception(f'`{inspect.currentframe().f_code.co_name}()`: Caught exception:'); raise


    def _update_candles(self, symbols: Optional[Iterable[Symbol]] = None):
        ThreadedTasks(
            self._update_contract_candles,
            ThreadedTasks.tupleize_single(
                symbols
                if symbols is not None else
                self.get_symbols()
            ),
        ).run()

    def _update_contract_candles(self, symbol: Symbol):
        contract = self[symbol]
        kline = self.pybit.fetch_kline(symbol, from_past_to_present=True)

        # confirmed (closed) candles
        contract.prices.update(
            kline.closes[:-1],
            kline.timestamps[1:],
            is_final=True,
        )
        contract.turnovers.update(
            kline.turnovers[:-1],
            kline.timestamps[1:],
            is_final=True,
        )

        # last unconfirmed (unknown status) candle
        contract.prices.update(
            kline.closes[-1],
            kline.timestamps[-1] + contract.prices.get_time_step(),
        )
        contract.turnovers.update(
            kline.turnovers[-1],
            kline.timestamps[-1] + contract.turnovers.get_time_step(),
        )
