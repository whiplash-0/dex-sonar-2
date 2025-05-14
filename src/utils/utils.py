import math
from enum import IntEnum, auto



class NumericUnit(IntEnum):
    def _generate_next_value_(name, start, count, last_values):
        return 10 ** (3 * count)

    ONE = auto()
    THOUSAND = auto()
    MILLION = auto()
    BILLION = auto()
    TRILLION = auto()
    QUADRILLION = auto()



UNIT_MAPPING = {
    'K': NumericUnit.THOUSAND,
    'M': NumericUnit.MILLION,
    'B': NumericUnit.BILLION,
    'T': NumericUnit.TRILLION,
    'Q': NumericUnit.QUADRILLION,
}
UNIT_LETTERS = list(UNIT_MAPPING.keys())


def format_large_number(x, decimal_places=0, decrease_decimal_places=False):
    if abs(x) < 1000:
        return f'{(int(x))}'
    else:
        unit = int(math.log10(abs(x))) // 3
        n = x / 10 ** (3 * unit)
        integer_digits = len(str(int(n)))
        final_decimal_places = decimal_places if not decrease_decimal_places else max(decimal_places - integer_digits + 1, 0)
        return f'{n:.{final_decimal_places}f}{UNIT_LETTERS[unit - 1]}'


def parse_large_number(string, as_type: type[int] | type[float] = float) -> int | float:
    return as_type(string) if not string[-1].isalpha() else as_type(string[:-1]) * UNIT_MAPPING[string[-1]]



def format_number_by_significant_digits(x, significant_digits=1, decimal_places=0, keep_leading_zeros=False):
    if x == 0:
        return '0'
    else:
        magnitude = math.floor(math.log10(abs(x)))
        factor = 10 ** (significant_digits - magnitude - 1)
        x = round(x * factor) / factor
        string = f'{x:.{decimal_places if decimal_places else max(0, -(magnitude - significant_digits + 1))}f}'

        if not keep_leading_zeros and '.' in string:
            integer_part, decimal_part = string.split('.')
            decimal_part = decimal_part.rstrip('0').ljust(decimal_places, '0')
            string = integer_part + ('.' + decimal_part if decimal_part else '')

        return string
