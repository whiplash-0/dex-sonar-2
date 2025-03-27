from math import log10
from typing import Callable


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
