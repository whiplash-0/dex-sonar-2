from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto
from typing import Callable, Optional

from src.pairs.pair import Pair, Turnover
from src.support.time_series import Index
from src.utils.time import Cooldowns


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


class Prefer(Enum):
    MAX_CHANGE = auto()
    SHORTER_RANGE = auto()


class SpikeDetector:
    def __init__(
            self,
            max_range: Range,
            threshold_function: Callable[[Range], Change],
            turnover_multiplier: Callable[[Turnover], float] = lambda _: 1,
            mode: Mode = Mode.BOTH,
            prefer: Prefer = Prefer.MAX_CHANGE,
            cooldown: timedelta = timedelta(),
    ):
        self.max_range = max_range
        self.threshold_function = threshold_function
        self.turnover_multiplier = turnover_multiplier
        self.mode = mode
        self.prefer = prefer
        self.pairs_cooldowns = Cooldowns(cooldown=cooldown)

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
            absolute_changes = [abs(x) for x in changes]

            if indices := [
                i
                for i, (x, y) in enumerate(zip(absolute_changes, thresholds))
                if x >= y
            ]:
                self.pairs_cooldowns.set_cooldown(pair)

                match self.prefer:
                    case Prefer.MAX_CHANGE:    spike_index = max(indices, key=lambda i: absolute_changes[i])
                    case Prefer.SHORTER_RANGE: spike_index = indices[0]
                    case _:                    spike_index = None

                return Spike(
                    change=changes[spike_index],
                    start=prices.get_normalized_index(-(spike_index + 1) - 1),  # +1 to align with candles instead of changes, -1 to align with negative indexing
                    end=prices.get_last_index(),
                )


        return None
