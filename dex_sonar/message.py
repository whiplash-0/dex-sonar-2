import math
from abc import ABC, abstractmethod

from aiogram.utils import markdown

from dex_sonar.bot import ImageBuffer, Text
from dex_sonar.pair import Pair
from dex_sonar.trend_detector import Trend


def format_number_by_significant_digits(x, digits=1):
    if x == 0:
        return '0'
    else:
        magnitude = math.floor(math.log10(abs(x)))
        factor = 10 ** (digits - magnitude - 1)
        x = round(x * factor) / factor
        return f'{x:.0f}' if magnitude - digits + 1 >= 0 else f'{x:g}'


unit_letters = {
    1: 'K',
    2: 'M',
    3: 'B',
    4: 'T',
    5: 'Q',
}

def format_large_number(x, decimal_places=0):
    if abs(x) < 1000:
        return str(int(x))
    else:
        unit = int(math.log10(abs(x))) // 3
        n = x / 10 ** (3 * unit)
        return f'{n:.{decimal_places}f}{unit_letters[unit]}'


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


class TrendMessage(Message):
    def __init__(self, pair: Pair, trend: Trend):
        # text
        lines = []

        def add_line(*strings):
            if len(strings) == 1: lines.append(strings[0])
            else: lines.append(f'{strings[0]}{" " * (self.LINE_WIDTH - len(strings[0]) - len(strings[1]))}{strings[1]}')

        add_line(pair.pretty_symbol, f'{trend.change:+.1%}')
        add_line('Price:', '$' + format_number_by_significant_digits(pair.price, digits=4))
        add_line('Turnover:', '$' + format_large_number(pair.turnover, decimal_places=1))
        add_line('Open interest:', format_large_number(pair.open_interest, decimal_places=0))
        add_line('Funding rate:', format_number_by_significant_digits(pair.funding_rate * 100, digits=1) + '%')

        text = markdown.code('\n'.join(lines)) + '\n' + markdown.code(' ' * 24) + markdown.link('Trade on Bybit', f'https://www.bybit.com/trade/usdt/{pair.symbol}')

        # image
        buffer = ImageBuffer()
        pair.create_chart(
            size=0.4,
            height_ratio=0.5,

            colors=[
                ('#4287f5', 0, trend.start - 1),
                ('#ff367c', trend.start, pair.prices.get_last_index()),
            ],
            price_as_percent=True,
            hide_turnover_ticks=True,

            size_price=3.3,
            size_turnover=1,
            size_tick=3.2,
            size_grid=2,

            alpha_tick=1,

            max_ticks_x=7,
            max_ticks_y=5,
        ).savefig(
            buffer,
            format='png',
            dpi=150,
            bbox_inches='tight',
            pad_inches=0.1,
        )

        super().__init__(text, buffer)
