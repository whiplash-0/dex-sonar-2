import inspect
import logging
from datetime import datetime, timedelta
from typing import Callable, Iterable

from pybit import unified_trading

from src.pairs.pair import Pair, Symbol, TimeSeries
from src.pairs.pairs import Pairs
from src.pairs.pybit_converters import Convert, Response
from src.utils import time


CATEGORY = 'linear'


logger = logging.getLogger(__name__)


class LivePairs(Pairs):
    def __init__(
            self,
            update_frequency: timedelta = timedelta(seconds=10),
            callback_on_update: Callable[[Pair], None] = lambda _: None,
            include_filter: Callable[[list[Pair]], Iterable[Pair]] = lambda pairs: sorted(pairs, key=lambda x: x.turnover, reverse=True)[:10],
    ):
        super().__init__()

        self.update_frequency = update_frequency
        self.callback_on_update = callback_on_update
        self.include_filter = include_filter

        self.requests = unified_trading.HTTP(
            testnet=False,
        )
        self.websocket = unified_trading.WebSocket(
            testnet=False,
            channel_type=CATEGORY,
        )
        self.last_update: dict[Symbol, datetime] = {}

        self._init()

    def is_connection_alive(self):
        return self.websocket.is_connected()

    def subscribe_to_stream(self):
        self._update_candles()
        self.websocket.ticker_stream(self.get_symbols(), self._handle_ticker_update)
        self.websocket.kline_stream(1, self.get_symbols(), self._handle_kline_update)

    def _init(self):
        pairs = Pairs()

        for ticker in Convert.get_tickers(self.requests.get_tickers(category=CATEGORY)):
            if not ticker.is_prelisted:
                pairs.update(Pair(
                    symbol=ticker.symbol,
                    prices=TimeSeries(step=timedelta(minutes=1)),
                    turnovers=TimeSeries(step=timedelta(minutes=1)),
                    turnover=ticker.turnover,
                    open_interest=ticker.open_interest,
                    funding_rate=ticker.funding_rate,
                    next_funding_time=ticker.next_funding_time,
                ))

        self.update(self.include_filter(pairs))
        self.last_update = {x: time.MIN_TIMESTAMP for x in self.pairs}

    def _handle_ticker_update(self, response: Response):
        try:
            symbol = response['data']['symbol']

            if time.get_timestamp() - self.last_update[symbol] >= self.update_frequency:
                self.last_update[symbol] = time.get_timestamp()
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

                self.callback_on_update(pair)

        except Exception:
            logger.exception(f'Callback `{inspect.currentframe().f_code.co_name}` caught exception'); raise

    def _handle_kline_update(self, response: Response):
        try:
            if response['data'][0]['confirm']:
                kline = Convert.stream_kline(response)

                self[kline.symbol].prices.update(
                    kline.close,
                    time.ceil_timestamp_minute(kline.end),
                    is_final=True,
                )
                self[kline.symbol].turnovers.update(
                    kline.turnover,
                    time.ceil_timestamp_minute(kline.end),
                    is_final=True,
                )

        except Exception:
            logger.exception(f'Callback `{inspect.currentframe().f_code.co_name}` caught exception'); raise

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
