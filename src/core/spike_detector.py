from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Optional

from src.contracts.contract import Contract, Turnover
from src.support.time_series import Index
from src.support.upspike_threshold import UpspikeThreshold
from src.utils.time import Cooldowns, Timedelta


Change = float  # relative
Range = int


@dataclass
class Spike:
    change: Change
    start: Index
    end: Index


class Catch(Enum):
    ALL_SPIKES = auto()
    UPSPIKES_ONLY = auto()
    DOWNSPIKES_ONLY = auto()


class Prefer(Enum):
    MAX_CHANGE = auto()
    SHORTER_RANGE = auto()


class SpikeDetector:
    def __init__(
            self,
            max_range: Range,
            threshold_function: Callable[[Range], Change],
            turnover_multiplier: Callable[[Turnover], float] = lambda _: 1,
            catch: Catch = Catch.ALL_SPIKES,
            prefer: Prefer = Prefer.MAX_CHANGE,
            cooldown: Timedelta = Timedelta(),
    ):
        self.max_range = max_range
        self.threshold_function = threshold_function
        self.turnover_multiplier = turnover_multiplier
        self.catch = catch
        self.prefer = prefer
        self.contracts_cooldowns = Cooldowns(cooldown=cooldown)

    def detect(self, contract: Contract) -> Optional[Spike]:
        if not self.contracts_cooldowns.is_in_cooldown(contract):

            # calculate changes and minimal thresholds over given time range
            prices = contract.prices
            actual_max_range = min(self.max_range, len(prices) - 1)  # avoid having max range longer than actual range
            changes = [(contract.price - x) / x for x in prices[-2:-(actual_max_range + 1) - 1:-1]]  # from first change (2-nd candle) to last

            if self.catch is not Catch.ALL_SPIKES:  # trick to make Mode work and include only relevant changes
                changes = [max(x, 0) if self.catch is Catch.UPSPIKES_ONLY else min(x, 0) for x in changes]

            thresholds = [  # align ordinal with minute duration that function accepts by adding 1
                self.threshold_function(1 + i) * self.turnover_multiplier(contract.turnover) * UpspikeThreshold.get()
                for i in range(len(changes))
            ]


            # find indices where changes are above corresponding thresholds
            absolute_changes = [abs(x) for x in changes]

            if indices := [
                i
                for i, (x, y) in enumerate(zip(absolute_changes, thresholds))
                if x >= y
            ]:
                self.contracts_cooldowns.set_for(contract)

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
