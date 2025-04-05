import math
from math import log10
from typing import Callable



def format_number_by_significant_digits(x, digits=1):
    if x == 0:
        return '0'
    else:
        magnitude = math.floor(math.log10(abs(x)))
        factor = 10 ** (digits - magnitude - 1)
        x = round(x * factor) / factor
        return f'{x:.0f}' if magnitude - digits + 1 >= 0 else f'{x:g}'


UNIT_LETTERS = 'KMBTQ'

def format_large_number(x, decimal_places=0, decrease_decimal_places=False):
    if abs(x) < 1000:
        return f'{(int(x))}'
    else:
        unit = int(math.log10(abs(x))) // 3
        n = x / 10 ** (3 * unit)
        integer_digits = len(str(int(n)))
        final_decimal_places = decimal_places if not decrease_decimal_places else max(decimal_places - integer_digits + 1, 0)
        return f'{n:.{final_decimal_places}f}{UNIT_LETTERS[unit - 1]}'



UnaryFunction = Callable[[float], float]


def create_linear_piecewise_interpolation(*points) -> UnaryFunction:
    xs, ys = [p[0] for p in points], [p[1] for p in points]

    def linear_piecewise_function(x):
        for i in range(len(xs) - 1):
            if xs[i] <= x <= xs[i + 1]:
                x1, y1 = xs[i], ys[i]
                x2, y2 = xs[i + 1], ys[i + 1]
                return y1 + (x - x1) * (y2 - y1) / (x2 - x1)

        raise ValueError(f'{x} is outside of interpolation range.')

    return linear_piecewise_function


def create_turnover_based_log_scaling(base, low_scale, high_scale) -> UnaryFunction:
    return lambda x: 1 / (
            (low_scale if x < base else high_scale) ** (log10(x) - log10(base))
    )
