import asyncio
import inspect
import logging
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Callable, Iterable, Optional

from pybit import unified_trading
from pydantic import BaseModel, Field, field_validator, model_validator
from requests import exceptions as requests_exceptions


# pybit keywords
# response
RESULT = 'result'
LIST = 'list'
DATA = 'data'

# parameters
NEXT_PAGE_CURSOR = 'nextPageCursor'
TOPIC = 'topic'
SYMBOL = 'symbol'
CONFIRM = 'confirm'


logger = logging.getLogger(__name__)


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
    symbol: Symbol = Field(...)
    price: float = Field(..., alias='lastPrice')
    open_interest: float = Field(..., alias='openInterest')
    turnover: float = Field(..., alias='turnover24h')
    funding_rate: Optional[float] = Field(..., alias='fundingRate')
    next_funding_time: datetime = Field(..., alias='nextFundingTime')
    best_ask_price: Optional[float] = Field(..., alias='ask1Price')
    best_bid_price: Optional[float] = Field(..., alias='bid1Price')
    best_ask_size: Optional[float] = Field(..., alias='ask1Size')
    best_bid_size: Optional[float] = Field(..., alias='bid1Size')

    @model_validator(mode='before')
    @classmethod
    def _replace_empty_strings_with_none(cls, fields: dict) -> dict:
        for name, value in fields.items():
            if value == '': fields[name] = None
        return fields


class StreamTicker(Ticker):
    cross_sequence: int = Field(..., alias='cs')
    timestamp: datetime = Field(..., alias='ts')



class Kline(BaseModel):
    starts: list[datetime] = Field(...)
    opens: list[float] = Field(...)
    highs: list[float] = Field(...)
    lows: list[float] = Field(...)
    closes: list[float] = Field(...)
    volumes: list[float] = Field(...)
    turnovers: list[float] = Field(...)


class StreamKline(BaseModel):
    symbol: Symbol = Field(...)
    start: datetime = Field(..., alias='start')
    end: datetime = Field(..., alias='end')
    open: float = Field(..., alias='open')
    close: float = Field(..., alias='close')
    high: float = Field(..., alias='high')
    low: float = Field(..., alias='low')
    volume: float = Field(..., alias='volume')
    turnover: float = Field(..., alias='turnover')
    confirm: bool = Field(..., alias='confirm')



class Contract(str, Enum):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/enum#status
    """
    def _generate_next_value_(name, start, count, last_values):
        return ''.join([word.title() for word in name.split('_')])

    LINEAR_PERPETUAL = auto()
    LINEAR_FUTURES = auto()


class Status(str, Enum):
    """
    Refer to: https://bybit-exchange.github.io/docs/v5/enum#status
    """
    def _generate_next_value_(name, start, count, last_values):
        return ''.join([word.title() for word in name.split('_')])

    PRE_LAUNCH = auto()
    TRADING = auto()
    CLOSED = auto()


HOUR_IN_MINUTES = 60

class InstrumentInfo(BaseModel):
    """
    :param funding_interval: In hours
    """
    symbol: Symbol = Field(...)

    base_coin: str = Field(..., alias='baseCoin')
    quote_coin: str = Field(..., alias='quoteCoin')

    contract: Contract = Field(..., alias='contractType')
    status: Status = Field(..., alias='status')
    launch_time: datetime = Field(..., alias='launchTime')
    delisting_time: Optional[datetime] = Field(..., alias='deliveryTime')

    funding_interval: int = Field(..., alias='fundingInterval')

    @field_validator('delisting_time', mode='before')
    def _replace_zero_with_none(cls, v: str) -> Optional[str]:
        return None if v == '0' else v

    @field_validator('funding_interval', mode='before')
    def _convert_to_hours(cls, v: int) -> int:
        quotient, remainder = divmod(v, HOUR_IN_MINUTES)
        if remainder != 0: raise ValueError('`funding_interval` must be divisible by 60 (minutes)')
        return quotient



# contracts
CATEGORY = 'linear'
QUOTE_COIN = 'USDT'

# arguments
LIMIT = 1000
KLINE_INTERVAL = '1'


class PybitWrapper:
    def __init__(self):
        self.http = unified_trading.HTTP(testnet=False)
        self.websocket = unified_trading.WebSocket(testnet=False, channel_type=CATEGORY)

    def is_connection_alive(self):
        return self.websocket.is_connected()

    def subscribe_to_ticker_updates(self, symbols: Iterable[Symbol], callback: Callable[[Response], None]):
        self.websocket.ticker_stream(symbols, callback)

    def subscribe_to_kline_updates(self, symbols: Iterable[Symbol], callback: Callable[[Response], None]):
        self.websocket.kline_stream(KLINE_INTERVAL, symbols, callback)

    async def get_instruments_info(self, retries_on_error: int = 0, retry_cooldown: timedelta = timedelta()) -> dict[Symbol, InstrumentInfo]:
        response_list = []
        response = None

        while response is None or response[NEXT_PAGE_CURSOR] != '':  # ensure there are no more pages

            for i in range(1 + retries_on_error):
                try:
                    response = self.http.get_instruments_info(
                        category=CATEGORY,
                        limit=LIMIT,
                    )[RESULT]
                    response_list.extend(response[LIST])
                    break

                except (
                        requests_exceptions.ReadTimeout,
                        requests_exceptions.ConnectionError
                ) as e:
                    logger.warning(
                        f'{inspect.currentframe().f_code.co_name}(): Got {e}' +
                        (f'. Retrying in {retry_cooldown.total_seconds():.1f}s' if i < retries_on_error else '')
                    )

                    if i == retries_on_error:
                        raise

                    await asyncio.sleep(retry_cooldown.total_seconds())

        instruments_info = [
            InstrumentInfo(**x) for x in response_list
        ]

        return {
            x.symbol: x for x in instruments_info
            if (
                    x.contract is Contract.LINEAR_PERPETUAL and
                    x.quote_coin == QUOTE_COIN
            )
        }

    def get_tickers(self) -> dict[Symbol, Ticker]:
        tickers = [
            Ticker(**x)
            for x in self.http.get_tickers(category=CATEGORY)[RESULT][LIST]
        ]
        return {x.symbol: x for x in tickers}

    def get_kline(self, symbol: Symbol, from_past_to_present: bool = False) -> Kline:
        response_list = self.http.get_kline(
            category=CATEGORY,
            symbol=symbol,
            intervalTime=KLINE_INTERVAL,
            limit=LIMIT,
        )[RESULT][LIST]

        kline = list(
            zip(*(
                response_list
                if not from_past_to_present else
                reversed(response_list)
            ))
        )

        return Kline(
            starts=kline[0],
            opens=kline[1],
            highs=kline[2],
            lows=kline[3],
            closes=kline[4],
            volumes=kline[5],
            turnovers=kline[6],
        )

    @staticmethod
    def parse_stream_ticker(response: Response) -> StreamTicker:
        return StreamTicker(**response, **response[DATA])

    @staticmethod
    def parse_stream_kline(response: Response) -> StreamKline:
        return StreamKline(symbol=response[TOPIC].rsplit('.', 1)[-1], **response[DATA][0])
