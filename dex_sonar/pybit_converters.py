from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, model_validator


Response = dict
Symbol = str


class Ticker(BaseModel):
    symbol: Symbol = Field(...)
    price: float = Field(..., alias='lastPrice')
    open_interest: float = Field(..., alias='openInterest')
    turnover: float = Field(..., alias='turnover24h')
    funding_rate: Optional[float] = Field(..., alias='fundingRate')
    next_funding_time: datetime = Field(..., alias='nextFundingTime')
    best_ask_price: float = Field(..., alias='ask1Price')
    best_bid_price: float = Field(..., alias='bid1Price')
    best_ask_size: float = Field(..., alias='ask1Size')
    best_bid_size: float = Field(..., alias='bid1Size')

    @model_validator(mode='before')
    @classmethod
    def replace_empty_fields_with_none(cls, fields: dict) -> dict:
        for name, value in fields.items():
            if value == '': fields[name] = None
        return fields

def convert_get_tickers(response: Response) -> list[Ticker]:
    return [Ticker(**x) for x in response['result']['list']]


class StreamTicker(Ticker):
    cross_sequence: int = Field(..., alias='cs')
    timestamp: datetime = Field(..., alias='ts')

def convert_stream_ticker(response: Response) -> StreamTicker:
    return StreamTicker(**response, **response['data'])


class Kline(BaseModel):
    starts: list[datetime] = Field(...)
    opens: list[float] = Field(...)
    highs: list[float] = Field(...)
    lows: list[float] = Field(...)
    closes: list[float] = Field(...)
    volumes: list[float] = Field(...)
    turnovers: list[float] = Field(...)

def convert_get_kline(response: Response, from_past_to_present: bool = False) -> Kline:
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

def convert_stream_kline(response: Response) -> StreamKline:
    return StreamKline(symbol=response['topic'].rsplit('.', 1)[-1], **response['data'][0])
