import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, Hashable, TypeVar


Seconds = float
Timestamp = datetime  # timezone aware by convention, otherwise UTC timezone is assumed
Timedelta = timedelta


MIN_TIMESTAMP = Timestamp.min.replace(tzinfo=timezone.utc)



def get_timestamp() -> Timestamp:
    return Timestamp.now(timezone.utc)


def get_monotonic() -> Seconds:
    return time.monotonic()


def get_time_passed_since(ts: Timestamp) -> Timedelta:
    return Timestamp.now(timezone.utc) - ts


def ceil_timestamp_minute(ts: Timestamp) -> Timestamp:
    ceiled_part = Timedelta(seconds=ts.second, microseconds=ts.microsecond)
    return ts if not ceiled_part else ts - ceiled_part + Timedelta(minutes=1)



T = TypeVar('T', bound=Hashable)


class Cooldowns(Generic[T]):
    def __init__(self, cooldown: Timedelta):
        self.cooldown = cooldown
        self.cooldown_starts: dict[T, Timestamp] = {}

    def get_cooldown(self) -> Timedelta:
        return self.cooldown

    def set_for(self, key: T):
        self.cooldown_starts[key] = get_timestamp()

    def set_start_for(self, key: T, timestamp: Timestamp):
        self.cooldown_starts[key] = timestamp

    def is_in_cooldown(self, key: T) -> bool:
        return get_time_passed_since(self.cooldown_starts.get(key, MIN_TIMESTAMP)) <= self.cooldown



@dataclass
class _TimeUnit:
    name: str
    time: Timedelta

    def format(self, units: int, shorten: bool = False) -> str:
        return f'{units}{(" " if not shorten else "")}{self.name if not shorten else self.name[0]}{"" if units == 1 or shorten else "s"}'


_time_units = [
    _TimeUnit(*x) for x in
    [
        ('second', Timedelta(seconds=1)),
        ('minute', Timedelta(minutes=1)),
        ('hour',   Timedelta(hours=1)),
        ('day',    Timedelta(days=1)),
        ('month',  Timedelta(days=30)),
        ('year',   Timedelta(days=365)),
    ]
]


def format_timedelta(td: Timedelta, shorten: bool = False) -> str:
    for tu in reversed(_time_units):
        if td >= tu.time: return tu.format(td // tu.time, shorten)
    return _time_units[0].format(0, shorten)
