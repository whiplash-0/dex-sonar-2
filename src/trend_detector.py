from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Optional

from src import time
from src.pair import Pair, Turnover
from src.time_series import Index


Change = float
Range = int


@dataclass
class Trend:
    change: Change
    start: Index
    end: Index
    is_weak: bool

    @property
    def is_normal(self):
        return not self.is_weak


class TrendDetector:
    def __init__(
            self,
            max_range: Range,
            absolute_change_threshold: Callable[[Range], Change],
            turnover_multiplier: Callable[[Turnover], float] = lambda _: 1,
            weak_trend_threshold: Change = 1,
            cooldown: timedelta = timedelta(),
    ):
        self.max_range = max_range
        self.absolute_change_threshold = absolute_change_threshold
        self.turnover_multiplier = turnover_multiplier
        self.weak_trend_threshold = weak_trend_threshold
        self.cooldown = cooldown
        self.last_detection: dict[(Pair, bool), datetime] = {}

    def detect(self, pair: Pair) -> Optional[Trend]:
        if self._is_in_cooldown(pair, is_weak=False):
            return None

        prices = pair.prices
        max_range_ = min(self.max_range, len(prices) - 1)
        changes = [abs((pair.price - x) / x) for x in prices[-2:-max_range_ - 1 - 1:-1]]
        thresholds = [self.absolute_change_threshold(i + 1) * self.turnover_multiplier(pair.turnover) for i, x in enumerate(changes)]


        indices = [i for i, (x, y) in enumerate(zip(changes, thresholds)) if x >= y]
        change_index = indices[-1] if indices else None
        is_weak = None

        if change_index is not None:
            is_weak = False

        elif not self._is_in_cooldown(pair, is_weak=True):
            indices = [i for i, (x, y) in enumerate(zip(changes, thresholds)) if x >= y * self.weak_trend_threshold]
            change_index = indices[-1] if indices else None

            if change_index is not None:
                is_weak = True


        if change_index is not None:
            self.last_detection[pair, is_weak] = time.get_timestamp()
            return Trend(
                change=changes[change_index],
                start=prices.get_normalized_index(-change_index - 2),
                end=prices.get_last_index(),
                is_weak=is_weak,
            )

        return None

    def _is_in_cooldown(self, pair: Pair, is_weak=False):
        last_detection_time = self.last_detection.get((pair, is_weak), time.MIN_TIMESTAMP)
        return time.get_time_passed_since(last_detection_time) <= self.cooldown
