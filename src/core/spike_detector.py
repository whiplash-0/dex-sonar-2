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
            cooldown: timedelta = timedelta(),
            mode: Mode = Mode.BOTH,
    ):
        self.max_range = max_range
        self.absolute_change_threshold = absolute_change_threshold
        self.turnover_multiplier = turnover_multiplier
        self.cooldown = cooldown
        self.mode = mode
        self.last_detection: dict[Pair, datetime] = {}

    def detect(self, pair: Pair) -> Optional[Spike]:
        if not self._is_in_cooldown(pair):

            # calculate changes and minimal thresholds over given time range
            prices = pair.prices
            max_range_ = min(self.max_range, len(prices) - 1)
            changes = [(pair.price - x) / x for x in prices[-2:-max_range_ - 1 - 1:-1]]

            if self.mode is not Mode.BOTH:  # trick to make Mode work and include only relevant changes
                changes = [max(x, 0) if self.mode is Mode.UPSPIKE else min(x, 0) for x in changes]

            thresholds = [self.absolute_change_threshold(1 + i) * self.turnover_multiplier(pair.turnover) for i in range(len(changes))]  # align ordinal with minute duration that function accepts by adding 1

            # find indices where changes are above corresponding thresholds
            indices = [
                i
                for i, (x, y) in enumerate(zip(changes, thresholds))
                if abs(x) >= y
            ]
            change_index = indices[0] if indices else None  # use first (shortest) change that is above threshold

            # create spike
            if change_index is not None:
                self.last_detection[pair] = time.get_timestamp()
                return Spike(
                    change=changes[change_index],
                    start=prices.get_normalized_index(-change_index - 2),
                    end=prices.get_last_index(),
                )

        return None

    def _is_in_cooldown(self, pair):
        return time.get_time_passed_since(self.last_detection.get(pair, time.MIN_TIMESTAMP)) <= self.cooldown
