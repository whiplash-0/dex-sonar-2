from datetime import datetime, timedelta
from typing import Callable

from pybit.unified_trading import HTTP, WebSocket

from dex_sonar import time
from dex_sonar.pair import Pair, Symbol, TimeSeries
from dex_sonar.pybit_converters import Response, convert_get_kline, convert_get_tickers, convert_stream_kline, convert_stream_ticker


Pairs = list[Pair]


class LivePairs:
    def __init__(
            self,
            update_frequency: timedelta = timedelta(seconds=10),
            callback_on_update: Callable[[], None] = lambda: None,
            include_filter: Callable[[Pairs], Pairs] = lambda pairs: sorted(pairs, key=lambda x: x.turnover, reverse=True)[:3],
    ):
        self.pairs: dict[Symbol, Pair] = {}
        self.update_frequency = update_frequency
        self.callback_on_update = callback_on_update
        self.include_filter = include_filter
        self.last_update: dict[Symbol, datetime] = {}

        self.requests = HTTP(
            testnet=False,
        )
        self.websocket = WebSocket(
            testnet=False,
            channel_type='linear',
        )

        self._init()

    def __len__(self):
        return len(self.pairs)

    def __getitem__(self, key) -> Pair:
        return self.pairs[key]

    def get_symbols(self) -> list[Symbol]:
        return [x.symbol for x in sorted(self.pairs.values(), key=lambda x: x.turnover, reverse=True)]

    def subscribe_to_stream(self):
        self._update_klines()
        self.websocket.kline_stream(1, self.pairs.keys(), self._handle_kline_update)
        self.websocket.ticker_stream(self.pairs.keys(), self._handle_ticker_update)

    def close_connection(self):
        self.websocket.exit()

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

        self.pairs = {x.symbol: x for x in self.include_filter(pairs)}
        self.last_update = {x: time.MIN_TIMESTAMP for x in self.pairs}

    def _update_kline(self, symbol: Symbol):
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
        self.pairs[symbol].prices.update(
            kline.closes[:-1],
            kline.starts[1:],
            is_final=True,
        )
        self.pairs[symbol].turnovers.update(
            kline.turnovers[:-1],
            kline.starts[1:],
            is_final=True,
        )

        # last unconfirmed (unknown status) candle
        self.pairs[symbol].prices.update(
            kline.closes[-1],
            kline.starts[-1] + self.pairs[symbol].prices.get_time_step(),
        )
        self.pairs[symbol].turnovers.update(
            kline.turnovers[-1],
            kline.starts[-1] + self.pairs[symbol].turnovers.get_time_step(),
        )

    def _update_klines(self):
        for symbol in self.pairs: self._update_kline(symbol)

    def _handle_kline_update(self, response: Response):
        if response['data'][0]['confirm']:
            kline = convert_stream_kline(response)
            self.pairs[kline.symbol].prices.update(
                kline.close,
                time.ceil_timestamp_minute(kline.end),
                is_final=True,
            )
            self.pairs[kline.symbol].turnovers.update(
                kline.turnover,
                time.ceil_timestamp_minute(kline.end),
                is_final=True,
            )

    def _handle_ticker_update(self, response: Response):
        symbol = response['data']['symbol']

        if time.get_timestamp() - self.last_update[symbol] >= self.update_frequency:
            self.last_update[symbol] = time.get_timestamp()
            ticker = convert_stream_ticker(response)

            self.pairs[symbol].prices.update(
                ticker.price,
                time.ceil_timestamp_minute(ticker.timestamp),
            )
            self.pairs[symbol].update(
                turnover=ticker.turnover,
                open_interest=ticker.open_interest,
                funding_rate=ticker.funding_rate,
                next_funding_time=ticker.next_funding_time,
            )

            self.callback_on_update()
