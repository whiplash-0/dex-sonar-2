from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Callable, Optional

from src.pairs.pair import Pair, Turnover
from src.support.time_series import Index
from src.utils import time


Change = float
Range = int


@dataclass
class Spike:
    change: Change
    start: Index
    end: Index
    is_weak: bool

    @property
    def is_normal(self):
        return not self.is_weak


class Mode(Enum):
    BOTH = auto()
    UPSPIKE = auto()
    DOWNSPIKE = auto()


class SpikeDetector:
    def __init__(
            self,
            max_range: Range,
            absolute_change_threshold: Callable[[Range], Change],
            turnover_multiplier: Callable[[Turnover], float] = lambda _: 1,
            weak_spike_threshold: Change = 1,
            cooldown: timedelta = timedelta(),
            mode: Mode = Mode.BOTH,
    ):
        self.max_range = max_range
        self.absolute_change_threshold = absolute_change_threshold
        self.turnover_multiplier = turnover_multiplier
        self.weak_spike_threshold = weak_spike_threshold
        self.cooldown = cooldown
        self.mode = mode
        self.last_detection: dict[(Pair, bool), datetime] = {}

    def detect(self, pair: Pair) -> Optional[Spike]:
        if not self._is_in_cooldown(pair):

            # calculate changes and minimal thresholds over given time range
            prices = pair.prices
            max_range_ = min(self.max_range, len(prices) - 1)
            changes = [(pair.price - x) / x for x in prices[-2:-max_range_ - 1 - 1:-1]]

            if self.mode is not Mode.BOTH:  # trick to make mode work and include only relevant changes
                changes = [max(x, 0) if self.mode is Mode.UPSPIKE else min(x, 0) for x in changes]

            thresholds = [self.absolute_change_threshold(1 + i) * self.turnover_multiplier(pair.turnover) for i in range(len(changes))]  # align ordinal with minute duration that function accepts by adding 1

            # find indices where changes are above corresponding thresholds
            is_weak = None

            if (change_index := self._find_exceeding_change_index(changes, thresholds)) is not None:  # check normal spike
                is_weak = False
            elif (  # else check weak spike
                    not self._is_in_cooldown(pair, is_weak=True)
                    and (change_index := self._find_exceeding_change_index(changes, thresholds, weak_threshold=True)) is not None
            ):
                is_weak = True

            # create spike
            if change_index is not None:
                self.last_detection[pair, is_weak] = time.get_timestamp()
                return Spike(
                    change=changes[change_index],
                    start=prices.get_normalized_index(-change_index - 2),
                    end=prices.get_last_index(),
                    is_weak=is_weak,
                )

        return None

    def _find_exceeding_change_index(self, changes, thresholds, weak_threshold=False) -> Optional[Index]:
        threshold_multiplier = 1 if not weak_threshold else self.weak_spike_threshold
        indices = [
            i
            for i, (x, y) in enumerate(zip(changes, thresholds))
            if abs(x) >= y * threshold_multiplier
        ]
        return indices[-1] if indices else None  # use first (shortest) change that satisfies condition

    def _is_in_cooldown(self, pair, is_weak=False):
        last_detection_time = self.last_detection.get((pair, is_weak), time.MIN_TIMESTAMP)
        return time.get_time_passed_since(last_detection_time) <= self.cooldown
