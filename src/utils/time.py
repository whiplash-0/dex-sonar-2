import importlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, Hashable, TypeVar

_time = importlib.import_module('time')



Seconds = float
Timestamp = datetime  # timezone aware by convention, otherwise UTC timezone is assumed
Timedelta = timedelta


MIN_TIMESTAMP = Timestamp.min.replace(tzinfo=timezone.utc)



class TimeUnit:
    MICROSECOND = Timedelta(microseconds=1)
    MILLISECOND = Timedelta(milliseconds=1)
    SECOND      = Timedelta(seconds=1)
    MINUTE      = Timedelta(minutes=1)
    HOUR        = Timedelta(hours=1)
    DAY         = Timedelta(days=1)
    WEEK        = Timedelta(weeks=1)
    MONTH       = Timedelta(days=30)   # approximation
    YEAR        = Timedelta(days=365)  # approximation



@dataclass
class _TimeUnit:
    name: str
    timedelta: Timedelta

    def format(self, units: int, shorten: bool = False) -> str:
        return f'{units}{(" " if not shorten else "")}{self.name if not shorten else self.name[0]}{"" if units == 1 or shorten else "s"}'


_time_units = [
    _TimeUnit(*x) for x in
    [
        ('second', TimeUnit.SECOND),
        ('minute', TimeUnit.MINUTE),
        ('hour',   TimeUnit.HOUR),
        ('day',    TimeUnit.DAY),
        ('month',  TimeUnit.MONTH),
        ('year',   TimeUnit.YEAR),
    ]
]



def get_timestamp() -> Timestamp:
    return Timestamp.now(timezone.utc)


def get_monotonic() -> Seconds:
    return _time.monotonic()


def get_time_passed_since(ts: Timestamp) -> Timedelta:
    return Timestamp.now(timezone.utc) - ts


def ceil_timestamp_minute(ts: Timestamp) -> Timestamp:
    ceiled_part = Timedelta(seconds=ts.second, microseconds=ts.microsecond)
    return ts if not ceiled_part else ts - ceiled_part + TimeUnit.MINUTE


def format_timedelta(td: Timedelta, shorten: bool = False) -> str:
    for tu in reversed(_time_units):
        if td >= tu.timedelta:
            return tu.format(td // tu.timedelta, shorten)
    return _time_units[0].format(0, shorten)



_T = TypeVar('_T', bound=Hashable)


class Cooldowns(Generic[_T]):
    def __init__(self, cooldown: Timedelta):
        self.cooldown = cooldown
        self.cooldown_starts: dict[_T, Timestamp] = {}

    def get_cooldown(self) -> Timedelta:
        return self.cooldown

    def set_for(self, key: _T):
        self.cooldown_starts[key] = get_timestamp()

    def set_start_for(self, key: _T, timestamp: Timestamp):
        self.cooldown_starts[key] = timestamp

    def is_in_cooldown(self, key: _T) -> bool:
        return get_time_passed_since(self.cooldown_starts.get(key, MIN_TIMESTAMP)) <= self.cooldown
