from datetime import datetime
from enum import Enum, auto
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


HOUR_IN_MINUTES = 60


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
    prelisting_phase: Optional[PrelistingPhase] = Field(..., alias='curPreListingPhase')

    @property
    def is_prelisted(self):
        return self.prelisting_phase is not None

    @model_validator(mode='before')
    @classmethod
    def replace_empty_fields_with_none(cls, fields: dict) -> dict:
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


class InstrumentInfo(BaseModel):
    symbol: Symbol = Field(...)
    funding_interval: int = Field(..., alias='fundingInterval')  # in hours

    @field_validator('funding_interval', mode='before')
    def _normalize_funding_interval(cls, v: int) -> int:
        if v % HOUR_IN_MINUTES != 0: raise ValueError('`funding_interval` must be divisible by 60 (minutes)')
        return int(v / HOUR_IN_MINUTES)


class Convert:
    @staticmethod
    def get_tickers(response: Response) -> list[Ticker]:
        return [Ticker(**x) for x in response['result']['list']]

    @staticmethod
    def stream_ticker(response: Response) -> StreamTicker:
        return StreamTicker(**response, **response['data'])

    @staticmethod
    def get_kline(response: Response, from_past_to_present: bool = False) -> Kline:
        data = response['result']['list']
        kline = list(zip(*(data if not from_past_to_present else reversed(data))))
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
    def stream_kline(response: Response) -> StreamKline:
        return StreamKline(symbol=response['topic'].rsplit('.', 1)[-1], **response['data'][0])

    @staticmethod
    def get_instruments_info(response: Response) -> list[InstrumentInfo]:
        return [InstrumentInfo(**x) for x in response['result']['list']]
