import importlib
from dataclasses import dataclass
from datetime import datetime, timedelta as _timedelta, timezone
from typing import Generic, Hashable, TypeVar

_time = importlib.import_module('time')



Seconds = float
Timestamp = datetime  # timezone aware by convention, otherwise UTC timezone is assumed
Timedelta = _timedelta
Timezone = timezone
TimeRange = tuple[Timestamp, Timestamp]



class TimestampBounds:
    MIN = Timestamp.min.replace(tzinfo=timezone.utc)
    MAX = Timestamp.max.replace(tzinfo=timezone.utc)



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


class Time:
    @staticmethod
    def now() -> Timestamp:
        return Timestamp.now(timezone.utc)

    @staticmethod
    def monotonic() -> Seconds:
        return _time.monotonic()

    @staticmethod
    def passed_since(timestamp: Timestamp) -> Timedelta:
        return Timestamp.now(timezone.utc) - timestamp

    @staticmethod
    def compute_intersection_duration(time_range1: TimeRange, time_range2: TimeRange) -> Timedelta:
        (s1, e1), (s2, e2) = time_range1, time_range2
        return max(
            (
                    min(e1, e2)
                    -
                    max(s1, s2)
            ),
            Timedelta(),
        )

    @staticmethod
    def ceil_to_minute(timestamp: Timestamp) -> Timestamp:
        ceiled_part = Timedelta(
            seconds=timestamp.second,
            microseconds=timestamp.microsecond,
        )
        return (
            timestamp
            if not ceiled_part else
            timestamp - ceiled_part + TimeUnit.MINUTE
        )

    @staticmethod
    def format_timedelta(timedelta: Timedelta, shorten: bool = False) -> str:
        for tu in reversed(_time_units):
            if timedelta >= tu.timedelta:
                return tu.format(timedelta // tu.timedelta, shorten)
        return _time_units[0].format(0, shorten)



_T = TypeVar('_T', bound=Hashable)


class Cooldowns(Generic[_T]):
    def __init__(self, cooldown: Timedelta):
        self.cooldown = cooldown
        self.cooldown_starts: dict[_T, Timestamp] = {}

    def get_cooldown(self) -> Timedelta:
        return self.cooldown

    def set_for(self, key: _T):
        self.cooldown_starts[key] = Time.now()

    def set_start_for(self, key: _T, timestamp: Timestamp):
        self.cooldown_starts[key] = timestamp

    def is_in_cooldown(self, key: _T) -> bool:
        return Time.passed_since(self.cooldown_starts.get(key, TimestampBounds.MIN)) <= self.cooldown
