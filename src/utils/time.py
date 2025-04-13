from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, Hashable, TypeVar


MIN_TIMESTAMP = datetime.min.replace(tzinfo=timezone.utc)


def get_timestamp() -> datetime:
    return datetime.now(timezone.utc)


def get_time_passed_since(ts: datetime) -> timedelta:
    return datetime.now(timezone.utc) - ts


def ceil_timestamp_minute(ts: datetime) -> datetime:
    ceiled_part = timedelta(seconds=ts.second, microseconds=ts.microsecond)
    return ts if not ceiled_part else ts - ceiled_part + timedelta(minutes=1)


T = TypeVar('T', bound=Hashable)

class Cooldowns(Generic[T]):
    def __init__(self, cooldown: timedelta):
        self.cooldown = cooldown
        self.cooldowns_starts: dict[T, datetime] = {}

    def set_cooldown(self, key: T):
        self.cooldowns_starts[key] = get_timestamp()

    def is_in_cooldown(self, key: T) -> bool:
        return get_time_passed_since(self.cooldowns_starts.get(key, MIN_TIMESTAMP)) <= self.cooldown


@dataclass
class _TimeUnit:
    name: str
    time: timedelta

    def format(self, units: int, shorten: bool = False) -> str:
        return f'{units}{(" " if not shorten else "")}{self.name if not shorten else self.name[0]}{"" if units == 1 or shorten else "s"}'

_time_units = [
    _TimeUnit(*x) for x in
    [
        ('second', timedelta(seconds=1)),
        ('minute', timedelta(minutes=1)),
        ('hour', timedelta(hours=1)),
        ('day', timedelta(days=1)),
        ('month', timedelta(days=30)),
        ('year', timedelta(days=365)),
    ]
]

def format_timedelta(td: timedelta, shorten: bool = False) -> str:
    for tu in reversed(_time_units):
        if td >= tu.time: return tu.format(td // tu.time, shorten)
    return _time_units[0].format(0, shorten)
