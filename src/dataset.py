import logging
from datetime import timezone
from functools import total_ordering
from pathlib import Path
from typing import Iterable, Optional, Self
from zoneinfo import ZoneInfo

import numpy as np

from src.contracts.pybit_wrapper import InstrumentInfo, PybitWrapper, Symbol
from src.core.workflow_runner import ThreadedTasks
from src.support import logs
from src.utils.time import Time, TimeUnit, Timedelta, Timestamp



logs.setup_logging(
    level=logging.INFO,
    format='%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s',
    timestamp_format='%m.%d %H:%M:%S',
    timezone=ZoneInfo('Europe/Prague'),
    forward_to_stdout=True,
)
logger = logging.getLogger(__name__)



@total_ordering
class YearMonth:
    MONTHS_IN_YEAR = 12

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month

    def __eq__(self, other: Self):
        return (self.year, self.month) == (other.year, other.month)

    def __lt__(self, other: Self):
        return (self.year, self.month) < (other.year, other.month)

    def __iadd__(self, x: int):
        months = (self.year * self.MONTHS_IN_YEAR) + (self.month - 1) + x
        year, month_zero_based = divmod(months, self.MONTHS_IN_YEAR)
        self.year, self.month = year, 1 + month_zero_based
        return self

    def __add__(self, x: int):
        return self.copy().__iadd__(x)

    def __sub__(self, x: int):
        return self + (-x)

    def __repr__(self):
        return f'{type(self).__name__}({self.year}, {self.month})'

    @classmethod
    def from_timestamp(cls, timestamp: Timestamp) -> Self:
        return cls(timestamp.year, timestamp.month)

    @classmethod
    def now(cls) -> Self:
        return cls.from_timestamp(Time.now())

    @classmethod
    def generate_range(cls, start: Self, end: Self, reverse=False) -> Iterable[Self]:
        items = []
        were_swapped = False

        if start > end:
            were_swapped = True
            start, end = end, start

        current = start.copy()

        while current <= end:
            items.append(current.copy())
            current += 1

        return reversed(items) if reverse ^ were_swapped else items

    def copy(self) -> Self:
        return type(self)(self.year, self.month)

    def to_timestamp(self) -> Timestamp:
        return Timestamp(self.year, self.month, 1, tzinfo=timezone.utc)

    def to_end_timestamp(self) -> Timestamp:
        return (self + 1).to_timestamp() - Timedelta(microseconds=1)

    def format(self, specification: str) -> str:
        return self.to_timestamp().strftime(specification)



class Dataset:
    def __init__(self, path: str):
        self.path = Path(path).expanduser()
        self.pybit = PybitWrapper(
            retries_on_error=3,
            retry_cooldown=Timedelta(seconds=1),
        )

    async def generate(
            self,
            include_base_symbols: Optional[Iterable[Symbol]] = None,
            exclude_base_symbols: Optional[Iterable[Symbol]] = None,
            start: YearMonth = YearMonth.now(),
            end:   YearMonth = YearMonth(2018, 1),
            min_data_timespan: Optional[Timedelta] = None,
            overwrite=False,
            max_workers=100,
    ):
        if start > end:
            start_and_end_swapped = True
            start, end = end, start
        else:
            start_and_end_swapped = False

        # ensure there is minimal value
        min_data_timespan = max(
            min_data_timespan or Timedelta(),
            PybitWrapper.DATA_TIMEFRAME,
        )


        instruments_info: dict[Symbol, InstrumentInfo] = (
                  (await self.pybit.fetch_instruments_info(fix_launch_time=True))
                | (await self.pybit.fetch_instruments_info(fix_launch_time=True, delisted=True))
        )

        # filter instruments info
        for symbol in list(instruments_info.keys()):
            ii = instruments_info[symbol]

            # include / exclude
            if (
                    (
                            include_base_symbols
                            and ii.base_symbol not in include_base_symbols)
                    or
                    (
                            exclude_base_symbols
                            and ii.base_symbol     in exclude_base_symbols
                    )
            ):
                instruments_info.pop(symbol)
                continue

            # those are start timestamps so they need to be shifted
            ii.launch_time                          += TimeUnit.MINUTE
            if ii.delisting_time: ii.delisting_time += TimeUnit.MINUTE

            # filter out contracts outside time range
            if (
                    Time.compute_intersection_duration(
                        time_range1=(start.to_timestamp(), end.to_end_timestamp()),
                        time_range2=(ii.launch_time, ii.delisting_time or Time.now()),
                    ) < min_data_timespan
            ):
                instruments_info.pop(symbol)

        # squeeze start and end
        if instruments_info:
            start = YearMonth.from_timestamp(
                max(
                    min(
                        [x.launch_time for x in instruments_info.values()]
                    ),
                    start.to_timestamp(),
                )
            )
            end_timestamp = min(
                max(
                    [x.delisting_time if x.delisting_time else Time.now() for x in instruments_info.values()]
                ),
                end.to_end_timestamp(),
            )
            end = YearMonth.from_timestamp(
                end_timestamp
            )
            logger.info(f'Contracts: {len(instruments_info)} within {start.format("%Y.%m")} - {end.format("%Y.%m")}')
        else:
            logger.info(f'There are no contracts within time range: {start} - {end}. Dataset can\'t be generated')
            return


        # generate arguments
        task_args = []
        latest_timestamp = Time.now()

        for year_month in YearMonth.generate_range(start, end, reverse=start_and_end_swapped):

            segment_dir = self.path / year_month.format('%Y') / year_month.format('%m')
            segment_dir.mkdir(parents=True, exist_ok=True)

            task_args.extend(
                [
                    (
                        segment_dir,
                        year_month,
                        ii,
                        latest_timestamp,
                        overwrite,
                    )
                    for ii in instruments_info.values()
                    if (
                            year_month >= YearMonth.from_timestamp(ii.launch_time)
                            and (
                                    not ii.delisting_time
                                    or year_month <= YearMonth.from_timestamp(ii.delisting_time)
                            )
                    )
                ]
            )

        # run tasks
        ThreadedTasks(
            self._generate_segment,
            args=task_args,
            max_workers=max_workers,
        ).run()


        logger.info(f'Generated dataset: {self.path}')

    def _generate_segment(
            self,
            segment_dir: Path,
            year_month: YearMonth,
            ii: InstrumentInfo,
            latest_timestamp: Timestamp,
            overwrite: bool,
    ):
        file_path = segment_dir / (ii.base_symbol.upper() + '.npz')

        if not overwrite and file_path.exists():
            return

        prices = []
        turnovers = []
        segment_start = max(
            year_month.to_timestamp(),
            ii.launch_time,
        )
        start = segment_start - TimeUnit.MINUTE  # to include candle that has timestamp in previous month


        while (
                YearMonth.from_timestamp(start + TimeUnit.MINUTE) == year_month
                and (
                        not ii.delisting_time
                        or start < ii.delisting_time
                )
        ):
            if kline := self.pybit.fetch_kline(
                    ii.symbol,
                    start=start,
                    from_past_to_present=True,
            ):
                timestamps = kline.timestamps


                # ensure kline is continuous
                if timestamps[0] != start:
                    logging.error(
                        f'{ii.base_symbol}: Kline starts at {timestamps[0].strftime("%Y.%m.%d %H:%M")}, but it should start at {start.strftime("%Y.%m.%d %H:%M")}'
                    )
                    return

                differences = [
                    y - x
                    for x, y in zip(timestamps[:-1], timestamps[1:])
                ]

                if result := next(
                        (
                                (i, x)
                                for i, x in enumerate(differences)
                                if x != differences[0]
                        ),
                        None
                ):
                    i, difference = result
                    logging.error(
                        f'{ii.base_symbol}: There is gap between {timestamps[i].strftime("%Y.%m.%d %H:%M")} and {timestamps[i + 1].strftime("%Y.%m.%d %H:%M")}: {difference}'
                    )
                    return


                # ensure candles from next month won't be included
                n = next(
                    (
                        i
                        for i, x in enumerate(timestamps)
                        if (
                            x + TimeUnit.MINUTE >= (year_month + 1).to_timestamp()  # if it's from next month
                            or x + TimeUnit.MINUTE > latest_timestamp
                        )
                    ),
                    len(timestamps)
                )

                prices.extend(kline.closes[:n])
                turnovers.extend(kline.turnovers[:n])
                start = timestamps[n - 1] + TimeUnit.MINUTE

            else:
                break


        if prices and turnovers:
            np.savez_compressed(
                file_path,
                start=    segment_start.timestamp(),
                prices=   np.array(prices,    dtype=np.float32),
                turnovers=np.array(turnovers, dtype=np.float32),
            )
            logger.info(f'Generated: {year_month.format("%Y")}/{year_month.format("%m")}/{ii.base_symbol}')
        else:
            logger.error(f'There are no data for: {ii.symbol} in {year_month.format("%Y.%m")}')

