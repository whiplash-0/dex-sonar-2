from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter
from matplotlib.ticker import MaxNLocator, PercentFormatter

from dex_sonar.time_series import Index, TimeSeries


Symbol = str
Price = float
Turnover = float
OpenInterest = float


@dataclass
class Pair:
    symbol: Symbol
    prices: TimeSeries[Price]
    turnovers: TimeSeries[Turnover]
    turnover: Turnover
    open_interest: OpenInterest
    funding_rate: Optional[float]
    next_funding_time: datetime

    @property
    def price(self):
        return self.prices[-1]

    @property
    def pretty_symbol(self):
        return self.symbol[:-4] if self.symbol.endswith('USDT') else self.symbol

    def update(self, turnover, open_interest, funding_rate, next_funding_time):
        self.turnover = turnover
        self.open_interest = open_interest
        self.funding_rate = funding_rate
        self.next_funding_time = next_funding_time

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
                timestamps[start:end],
                self.prices[start:end],
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
        ax1.xaxis.set_major_formatter(DateFormatter(timestamp_format))
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
