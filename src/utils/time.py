import importlib
from dataclasses import dataclass
from datetime import datetime, timedelta as _timedelta, timezone
from enum import Enum
from functools import total_ordering
from typing import Generic, Hashable, TypeVar

_time = importlib.import_module('time')



Seconds = float
Timestamp = datetime  # timezone aware by convention, otherwise UTC timezone is assumed
Timedelta = _timedelta  # TODO: rename to TimeDelta and time_delta
Timezone = timezone
TimeRange = tuple[Timestamp, Timestamp]



class TimestampBounds:
    MIN = Timestamp.min.replace(tzinfo=timezone.utc)
    MAX = Timestamp.max.replace(tzinfo=timezone.utc)



# TODO: complete replacement
class OldTimeUnit:
    MICROSECOND = Timedelta(microseconds=1)
    MILLISECOND = Timedelta(milliseconds=1)
    SECOND      = Timedelta(seconds=1)
    MINUTE      = Timedelta(minutes=1)
    HOUR        = Timedelta(hours=1)
    DAY         = Timedelta(days=1)
    WEEK        = Timedelta(weeks=1)
    MONTH       = Timedelta(days=30)   # approximation
    YEAR        = Timedelta(days=365)  # approximation



@total_ordering
class TimeUnit(Enum):
    MICROSECOND = Timedelta(microseconds=1)
    MILLISECOND = Timedelta(milliseconds=1)
    SECOND      = Timedelta(seconds=1)
    MINUTE      = Timedelta(minutes=1)
    HOUR        = Timedelta(hours=1)
    DAY         = Timedelta(days=1)
    WEEK        = Timedelta(weeks=1)
    MONTH       = Timedelta(days=30)   # approximation
    YEAR        = Timedelta(days=365)  # approximation

    def __eq__(self, other):
        if isinstance(other, TimeUnit):  return self.value == other.value
        if isinstance(other, Timedelta): return self.value == other
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, TimeUnit):  return self.value < other.value
        if isinstance(other, Timedelta): return self.value < other
        return NotImplemented

    def __add__(self, other):
        if isinstance(other, TimeUnit):  return self.value + other.value
        if isinstance(other, Timedelta): return self.value + other
        return NotImplemented

    __radd__ = __add__

    def __mul__(self, other):
        if isinstance(other, (int, float)): return self.value * other
        return NotImplemented

    __rmul__ = __mul__

    def total_seconds(self) -> float:
        return self.value.total_seconds()



@dataclass
class _TimeUnit:
    name: str
    timedelta: Timedelta

    def format(self, units: int, shorten: bool = False) -> str:
        return f'{units}{(" " if not shorten else "")}{self.name if not shorten else self.name[0]}{"" if units == 1 or shorten else "s"}'


_time_units = [
    _TimeUnit(*x) for x in
    [
        ('second', OldTimeUnit.SECOND),
        ('minute', OldTimeUnit.MINUTE),
        ('hour', OldTimeUnit.HOUR),
        ('day', OldTimeUnit.DAY),
        ('month', OldTimeUnit.MONTH),
        ('year', OldTimeUnit.YEAR),
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
    def count_time_units(timedelta: Timedelta, time_unit: TimeUnit) -> int:
        return int(timedelta // time_unit)

    @staticmethod
    def ceil_to_minute(timestamp: Timestamp) -> Timestamp:
        ceiled_part = Timedelta(
            seconds=timestamp.second,
            microseconds=timestamp.microsecond,
        )
        return (
            timestamp
            if not ceiled_part else
            timestamp - ceiled_part + OldTimeUnit.MINUTE
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
