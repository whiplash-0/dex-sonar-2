from dataclasses import dataclass, field
from datetime import tzinfo
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter
from matplotlib.ticker import MaxNLocator, PercentFormatter

from src.support.time_series import Index, TimeSeries
from src.utils.time import Timedelta, Timestamp



CANDLE_TIMEFRAME = Timedelta(minutes=1)


Symbol = str
Price = float
Turnover = float



@dataclass
class Contract:
    """
    :param funding_interval: In hours
    """
    symbol: Symbol

    base_symbol: Symbol
    quote_symbol: Symbol

    launch_time: Timestamp
    delisting_time: Optional[Timestamp]

    prices: TimeSeries[Price] = field(init=False)
    turnovers: TimeSeries[Turnover] = field(init=False)

    turnover: Turnover
    funding_rate: Optional[float]
    funding_interval: int
    next_funding_time: Timestamp


    BASE_SYMBOL_MAX_LEN = 14


    def __post_init__(self):
        self.prices = TimeSeries(step=CANDLE_TIMEFRAME)
        self.turnovers = TimeSeries(step=CANDLE_TIMEFRAME)


    def __eq__(self, other):
        return other.symbol == self.symbol if isinstance(other, Contract) else False

    def __hash__(self):
        return hash(self.symbol)


    @property
    def is_being_delisted(self):
        return self.delisting_time is not None

    @property
    def price(self):
        return self.prices[-1]

    @property
    def funding_rate_per_day(self):
        return self.funding_rate / self.funding_interval * 24


    def create_chart(
            self,
            size=1,
            height_ratio=0.25,

            colors: str | Iterable[tuple[str, Index, Index]] = '#4287f5',
            price_as_percent=False,
            turnover_as_percent=False,
            hide_price_ticks=False,
            hide_turnover_ticks=False,
            time_on_top=False,
            timestamp_format='%H:%M',
            timezone: tzinfo = ZoneInfo('UTC'),

            size_price=1.0,
            size_turnover=1.0,
            size_tick=1.0,
            size_grid=1.0,

            alpha_tick=0.8,
            alpha_turnover=0.1,
            alpha_grid=0.2,

            max_ticks_x=None,
            max_ticks_y=None,
    ) -> plt.Figure:

        fig, ax1 = plt.subplots(figsize=(16 * size, 16 * size * height_ratio))
        ax1: plt.Axes
        ax2: plt.Axes = ax1.twinx()
        axes = ax1, ax2

        # create graphs
        timestamps = self.prices.get_timestamps()

        for color, start, end in [(colors, 0, self.prices.get_last_index())] if isinstance(colors, str) else colors:
            ax1.plot(
                timestamps[start:end + 1],
                self.prices[start:end + 1],
                color=color,
                linewidth=1.65 * size * size_price,
            )

        ax2.bar(
            self.turnovers.get_timestamps(),
            self.turnovers.get_values(),
            color='#000000',
            alpha=alpha_turnover,
            width=0.001 * size_turnover,
        )

        # remove edges
        for ax in axes:
            for edge in [
                'left',
                'right',
                'top',
                'bottom'
            ]:
                ax.spines[edge].set_visible(False)

        # remove margins
        for ax in axes: ax.margins(x=0, y=0)

        # remove ticks and move tick labels
        for ax in axes: ax.tick_params(left=False, bottom=False, right=False)
        ax1.tick_params(
            labelbottom=not time_on_top,
            labeltop=time_on_top,
            labelleft=False,
            labelright=not hide_price_ticks,
        )
        ax2.tick_params(
            labelbottom=False,
            labelleft=not hide_turnover_ticks,
            labelright=False,
        )

        # set tick size and opacity
        ax1.tick_params(axis='both', labelsize=10 * 1.1 * size * size_tick, colors=(0, 0, 0, alpha_tick))
        ax2.tick_params(axis='y', labelsize=10 * 1.1 * size * size_tick, colors=(0, 0, 0, alpha_tick))

        # format ticks
        ax1.xaxis.set_major_formatter(DateFormatter(timestamp_format, tz=timezone))
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'${x:,g}') if not price_as_percent else PercentFormatter(xmax=self.prices[-1], decimals=1))
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'${x:,.0f}') if not turnover_as_percent else PercentFormatter(xmax=self.turnovers[-1]))

        # limit the number of ticks
        if max_ticks_x: ax1.xaxis.set_major_locator(MaxNLocator(nbins=max_ticks_x))
        if max_ticks_y: ax1.yaxis.set_major_locator(MaxNLocator(nbins=max_ticks_y))
        if max_ticks_y: ax2.yaxis.set_major_locator(MaxNLocator(nbins=max_ticks_y))

        # add grid
        ax1.grid(
            color='#000000',
            linewidth=0.7 * size * size_grid,
            alpha=alpha_grid,
        )

        return fig
