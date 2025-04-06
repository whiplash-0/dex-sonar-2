from abc import ABC, abstractmethod
from datetime import tzinfo
from zoneinfo import ZoneInfo

from aiogram.utils import markdown
from matplotlib import pyplot as plt

from src.core.bot import ImageBuffer, Text
from src.core.spike_detector import Spike
from src.pairs.pair import Pair
from src.utils import time
from src.utils.utils import format_large_number, format_number_by_significant_digits


class Message(ABC):

    LINE_WIDTH = 35

    @abstractmethod
    def __init__(self, text: Text, image: ImageBuffer):
        self.text = text
        self.buffer = image

    def get_text(self) -> Text:
        return self.text

    def get_image(self) -> ImageBuffer:
        self.buffer.seek(0)
        return self.buffer


class SpikeMessage(Message):
    def __init__(self, pair: Pair, spike: Spike, timezone: tzinfo = ZoneInfo('UTC')):
        # text
        lines = []

        def add_line(*strings):
            if len(strings) == 1: lines.append(strings[0])
            else: lines.append(f'{strings[0]}{" " * (self.LINE_WIDTH - len(strings[0]) - len(strings[1]))}{strings[1]}')

        duration = time.format_timedelta(pair.prices.get_timestamp(spike.end) - pair.prices.get_timestamp(spike.start), shorten=True)
        add_line(pair.pretty_symbol, f'{spike.change:+.1%}/{duration}')
        add_line('Price:', '$' + format_number_by_significant_digits(pair.price, significant_digits=3))
        add_line('Turnover:', '$' + format_large_number(pair.turnover, decimal_places=1, decrease_decimal_places=True))
        add_line('Funding rate:', format_number_by_significant_digits(pair.funding_rate * 100, significant_digits=1, decimal_places=3) + '%')

        text = markdown.code('\n'.join(lines)) + '\n' + markdown.code(' ' * 24) + markdown.link('Trade on Bybit', f'https://www.bybit.com/trade/usdt/{pair.symbol}')

        # image
        buffer = ImageBuffer()
        fig = pair.create_chart(
            size=0.4,
            height_ratio=0.5,

            colors=[
                ('#4287f5', 0, spike.start),
                ('#ff367c', spike.start, pair.prices.get_last_index()),
            ],
            price_as_percent=True,
            hide_turnover_ticks=True,
            timezone=timezone,

            size_price=3.3,
            size_turnover=1,
            size_tick=3.2,
            size_grid=2,

            alpha_tick=1,

            max_ticks_x=7,
            max_ticks_y=5,
        )
        fig.savefig(
            buffer,
            format='png',
            dpi=150,
            bbox_inches='tight',
            pad_inches=0.1,
        )
        plt.close(fig)

        super().__init__(text, buffer)
