from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Callable, Optional

from src.pairs.pair import Pair, Turnover
from src.support.time_series import Index
from src.utils import time


Change = float  # relative
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


class PairCooldowns:
    def __init__(self, cooldown: timedelta):
        self.cooldown = cooldown
        self.cooldown_starts: dict[Pair, datetime] = {}

    def set_cooldown(self, pair: Pair):
        self.cooldown_starts[pair] = time.get_timestamp()

    def is_in_cooldown(self, pair: Pair):
        return time.get_time_passed_since(self.cooldown_starts.get(pair, time.MIN_TIMESTAMP)) <= self.cooldown


class SpikeDetector:
    def __init__(
            self,
            mode: Mode = Mode.BOTH,
            max_range: Range = 5,
            threshold_function: Callable[[Range], Change] = lambda _: 5,
            turnover_multiplier: Callable[[Turnover], float] = lambda _: 1,
            pairs_cooldowns: PairCooldowns = PairCooldowns(cooldown=timedelta()),
    ):
        self.mode = mode
        self.max_range = max_range
        self.threshold_function = threshold_function
        self.turnover_multiplier = turnover_multiplier
        self.pairs_cooldowns = pairs_cooldowns

    def detect(self, pair: Pair) -> Optional[Spike]:
        if not self.pairs_cooldowns.is_in_cooldown(pair):

            # calculate changes and minimal thresholds over given time range
            prices = pair.prices
            actual_max_range = min(self.max_range, len(prices) - 1)  # avoid having max range longer than actual range
            changes = [(pair.price - x) / x for x in prices[-2:-(actual_max_range + 1) - 1:-1]]  # from first change (2-nd candle) to last

            if self.mode is not Mode.BOTH:  # trick to make Mode work and include only relevant changes
                changes = [max(x, 0) if self.mode is Mode.UPSPIKE else min(x, 0) for x in changes]

            thresholds = [self.threshold_function(1 + i) * self.turnover_multiplier(pair.turnover) for i in range(len(changes))]  # align ordinal with minute duration that function accepts by adding 1

            # find indices where changes are above corresponding thresholds
            indices = [
                i
                for i, (x, y) in enumerate(zip(changes, thresholds))
                if abs(x) >= y
            ]
            change_index = indices[0] if indices else None  # use first (shortest) change that is above threshold

            # create and return spike
            if change_index is not None:
                self.pairs_cooldowns.set_cooldown(pair)
                return Spike(
                    change=changes[change_index],
                    start=prices.get_normalized_index(-(change_index + 1) - 1),  # +1 to align with candles instead of changes, -1 to align with negative indexing
                    end=prices.get_last_index(),
                )

        return None
