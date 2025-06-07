import asyncio
import inspect
import logging
from enum import Enum, auto
from typing import Callable, Iterable, Optional

from pybit import unified_trading
from pydantic import BaseModel, Field, field_validator, model_validator
from requests import exceptions as requests_exceptions

from src.core.workflow_runner import ThreadedTasks
from src.utils.time import TimeUnit, Timedelta, Timestamp



logger = logging.getLogger(__name__)


CORRECT_LAUNCH_TIME_MAX_WORKERS = 500



Response = dict
Symbol = str



class PrelistingPhase(str, Enum):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/enum#curauctionphase
    """
    def _generate_next_value_(name, start, count, last_values):
        return ''.join([word.title() for word in name.split('_')])

    NOT_STARTED = auto()
    CALL_AUCTION = auto()
    CONTINUOUS_TRADING = auto()
    CALL_AUCTION_NO_CANCEL = auto()
    CROSS_MATCHING = auto()
    FINISHED = auto()


class Ticker(BaseModel):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/market/tickers
    """
    symbol: Symbol = Field(...)

    price: float = Field(..., alias='lastPrice')
    open_interest: float = Field(..., alias='openInterest')
    turnover: float = Field(..., alias='turnover24h')
    funding_rate: Optional[float] = Field(..., alias='fundingRate')
    next_funding_time: Timestamp = Field(..., alias='nextFundingTime')

    best_ask_price: Optional[float] = Field(..., alias='ask1Price')
    best_ask_size: Optional[float] = Field(..., alias='ask1Size')
    best_bid_price: Optional[float] = Field(..., alias='bid1Price')
    best_bid_size: Optional[float] = Field(..., alias='bid1Size')

    @model_validator(mode='before')
    @classmethod
    def _replace_empty_strings_with_none(cls, fields: dict) -> dict:
        for name, value in fields.items():
            if value == '': fields[name] = None
        return fields


class StreamTicker(Ticker):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
    """
    cross_sequence: int = Field(..., alias='cs')
    timestamp: Timestamp = Field(..., alias='ts')



class Kline(BaseModel):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/market/kline
    """
    timestamps: list[Timestamp] = Field(...)
    opens: list[float] = Field(...)
    highs: list[float] = Field(...)
    lows: list[float] = Field(...)
    closes: list[float] = Field(...)
    volumes: list[float] = Field(...)
    turnovers: list[float] = Field(...)

    def __len__(self):
        return len(self.timestamps)


class StreamKline(BaseModel):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/websocket/public/kline
    """
    symbol: Symbol = Field(...)
    start: Timestamp = Field(..., alias='start')
    end: Timestamp = Field(..., alias='end')
    open: float = Field(..., alias='open')
    close: float = Field(..., alias='close')
    high: float = Field(..., alias='high')
    low: float = Field(..., alias='low')
    volume: float = Field(..., alias='volume')
    turnover: float = Field(..., alias='turnover')
    confirm: bool = Field(..., alias='confirm')



class Contract(str, Enum):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/enum#contracttype
    """
    def _generate_next_value_(name, start, count, last_values):
        return ''.join([word.title() for word in name.split('_')])

    LINEAR_PERPETUAL = auto()
    LINEAR_FUTURES = auto()


HOUR_IN_MINUTES = 60

class InstrumentInfo(BaseModel):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/market/instrument

    :param launch_time: Not reliable at least for `Kline` data. Use the corresponding method instead
    :param funding_interval: In hours
    """
    symbol: Symbol = Field(...)

    base_symbol: Symbol = Field(..., alias='baseCoin')
    quote_symbol: Symbol = Field(..., alias='quoteCoin')

    contract: Contract = Field(..., alias='contractType')
    launch_time: Timestamp = Field(..., alias='launchTime')
    delisting_time: Optional[Timestamp] = Field(..., alias='deliveryTime')

    funding_interval: int = Field(..., alias='fundingInterval')

    @field_validator('delisting_time', mode='before')
    def _replace_zero_with_none(cls, v: str) -> Optional[str]:
        return None if v == '0' else v

    @field_validator('funding_interval', mode='before')
    def _convert_to_hours(cls, v: int) -> int:
        quotient, remainder = divmod(v, HOUR_IN_MINUTES)
        if remainder != 0: raise ValueError('`funding_interval` must be divisible by 60 (minutes)')
        return quotient



RESULT = 'result'
LIST = 'list'
DATA = 'data'
CATEGORY = 'linear'
NEXT_PAGE_CURSOR = 'nextPageCursor'
LIMIT = 1000
KLINE_INTERVAL = '1'
DUMMY_OLD_TIMESTAMP = Timestamp(2000, 1, 1)


class PybitWrapper:
    DATA_TIMEFRAME = TimeUnit.MINUTE

    def __init__(self, retries_on_error: int = 0, retry_cooldown: Timedelta = Timedelta()):
        self.http = unified_trading.HTTP(testnet=False)
        self.websocket = unified_trading.WebSocket(testnet=False, channel_type=CATEGORY)
        self.retries_on_error = retries_on_error
        self.retry_cooldown = retry_cooldown
        self.cached_instruments_info = None

    def is_connection_alive(self):
        return self.websocket.is_connected()

    def subscribe_to_ticker_updates(self, symbols: Iterable[Symbol], callback: Callable[[Response], None]):
        self.websocket.ticker_stream(symbols, callback)

    def subscribe_to_kline_updates(self, symbols: Iterable[Symbol], callback: Callable[[Response], None]):
        self.websocket.kline_stream(KLINE_INTERVAL, symbols, callback)

    async def get_instruments_info(self, delisted=False, fix_launch_time=False, cached=False) -> dict[Symbol, InstrumentInfo]:
        """
        Should be used as only source for instruments / contracts, not `get_tickers()`
        """
        if cached and self.cached_instruments_info:
            return self.cached_instruments_info


        response_list = []
        response = None

        while response is None or response[NEXT_PAGE_CURSOR] != '':  # ensure there are no more pages

            for i in range(1 + self.retries_on_error):
                try:
                    response = self.http.get_instruments_info(
                        category=CATEGORY,
                        status='Trading' if not delisted else 'Closed',
                        limit=LIMIT,
                        cursor=response[NEXT_PAGE_CURSOR] if response else None,
                    )[RESULT]
                    response_list.extend(
                        response[LIST]
                    )
                    break

                except (
                        requests_exceptions.ReadTimeout,
                        requests_exceptions.ConnectionError
                ) as e:
                    logger.warning(
                        f'{inspect.currentframe().f_code.co_name}(): Caught exception: \'{e}\'' +
                        (f'. Retrying in {self.retry_cooldown.total_seconds():.1f}s' if i < self.retries_on_error else '')
                    )

                    if i == self.retries_on_error:
                        raise

                    await asyncio.sleep(self.retry_cooldown.total_seconds())


        self.cached_instruments_info = {  # filter only relevant contracts

            y.symbol: y for y in [
                InstrumentInfo(**x) for x in response_list
            ]
            if (
                    y.contract is Contract.LINEAR_PERPETUAL and
                    y.quote_symbol == 'USDT'
            )
        }


        if fix_launch_time:

            launch_times = ThreadedTasks(
                self._get_launch_time,
                ThreadedTasks.tupleize_single(self.cached_instruments_info.keys()),
                max_workers=CORRECT_LAUNCH_TIME_MAX_WORKERS,
            ).run()

            # correct launch_time, otherwise remove item since it's invalid (there are no Kline data)
            for symbol, launch_time in zip(
                    list(self.cached_instruments_info), launch_times  # use list to allow safe dictionary modification
            ):
                if launch_time:
                    self.cached_instruments_info[symbol].launch_time = launch_time
                else:
                    self.cached_instruments_info.pop(symbol)


        return self.cached_instruments_info

    def get_tickers(self) -> dict[Symbol, Ticker]:
        tickers = [
            Ticker(**x)
            for x in self.http.get_tickers(category=CATEGORY)[RESULT][LIST]
        ]
        return {x.symbol: x for x in tickers}

    def get_kline(
            self,
            symbol: Symbol,
            start: Optional[Timestamp] = None,
            end: Optional[Timestamp] = None,
            from_past_to_present: bool = False,
    ) -> Optional[Kline]:

        if start and end:  # ensure only 1 is passed
            raise ValueError('Only one of `start` or `end` may be provided, not both')

        response_list = self.http.get_kline(
            category=CATEGORY,
            symbol=symbol,
            start=start.timestamp() * 1000 if start else None,  # convert to milliseconds
            end=end.timestamp() * 1000 if end else None,
            interval=KLINE_INTERVAL,
            limit=LIMIT,
        )[RESULT][LIST]

        if response_list:
            kline = list(
                zip(*(
                    response_list
                    if not from_past_to_present else
                    reversed(response_list)
                ))
            )
            return Kline(
                timestamps=kline[0],
                opens=kline[1],
                highs=kline[2],
                lows=kline[3],
                closes=kline[4],
                volumes=kline[5],
                turnovers=kline[6],
            )

        return None

    def _get_launch_time(self, symbol: Symbol) -> Timestamp:
        if kline :=  self.get_kline(
            symbol,
            start=DUMMY_OLD_TIMESTAMP,
        ):
            return kline.timestamps[-1]
        else:
            return None

    @staticmethod
    def parse_stream_ticker(response: Response) -> StreamTicker:
        return StreamTicker(**response, **response[DATA])

    @staticmethod
    def parse_stream_kline(response: Response) -> StreamKline:
        return StreamKline(symbol=response['topic'].rsplit('.', 1)[-1], **response[DATA][0])

    @staticmethod
    def extract_symbol(response_stream_ticker: Response):
        return response_stream_ticker[DATA]['symbol']

    @staticmethod
    def is_candle_final(response_stream_kline: Response):
        return response_stream_kline[DATA][0]['confirm']
