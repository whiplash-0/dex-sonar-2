from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Callable, Iterable

from pybit.unified_trading import HTTP, WebSocket

from dex_sonar import time
from dex_sonar.pair import Pair, Symbol, TimeSeries
from dex_sonar.pairs import Pairs
from dex_sonar.pybit_converters import Response, convert_get_kline, convert_get_tickers, convert_stream_kline, convert_stream_ticker


class LivePairs(Pairs):
    def __init__(
            self,
            update_frequency: timedelta = timedelta(seconds=10),
            callback_on_update: Callable[[Pair], None] = lambda _: None,
            include_filter: Callable[[list[Pair]], Iterable[Pair]] = lambda pairs: sorted(pairs, key=lambda x: x.turnover, reverse=True)[:10],
    ):
        super().__init__()
        self.include_filter = include_filter
        self.update_frequency = update_frequency
        self.callback_on_update = callback_on_update
        self.last_update: dict[Symbol, datetime] = {}
        self.requests = HTTP(testnet=False)
        self._init()

    @contextmanager
    def subscribe_to_stream(self):
        ws = None

        try:
            ws = WebSocket(
                testnet=False,
                channel_type='linear',
            )
            self._update_klines()
            ws.kline_stream(1, self.get_symbols(), self._handle_kline_update)
            ws.ticker_stream(self.get_symbols(), self._handle_ticker_update)
            yield

        finally:
            ws.exit()

    def _init(self):
        pairs = []

        for ticker in convert_get_tickers(self.requests.get_tickers(category='linear')):
            pairs.append(Pair(
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

    def _update_klines(self):
        for symbol in self.get_symbols(): self._update_kline(symbol)

    def _update_kline(self, symbol: Symbol):
        pair = self[symbol]
        kline = convert_get_kline(
            self.requests.get_kline(
                category='linear',
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

    def _handle_kline_update(self, response: Response):
        if response['data'][0]['confirm']:
            kline = convert_stream_kline(response)
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

    def _handle_ticker_update(self, response: Response):
        symbol = response['data']['symbol']

        if time.get_timestamp() - self.last_update[symbol] >= self.update_frequency:
            self.last_update[symbol] = time.get_timestamp()
            ticker = convert_stream_ticker(response)
            pair = self[symbol]

            pair.prices.update(
                ticker.price,
                time.ceil_timestamp_minute(ticker.timestamp),
            )
            pair.update(
                turnover=ticker.turnover,
                open_interest=ticker.open_interest,
                funding_rate=ticker.funding_rate,
                next_funding_time=ticker.next_funding_time,
            )

            self.callback_on_update(pair)
