from typing import Iterable, Iterator, Optional, Self

from src.pairs.pair import Pair, Symbol


class Pairs:
    def __init__(self, pairs: Optional[Iterable[Pair]] = None):
        self.pairs: dict[Symbol, Pair] = dict((p.symbol, p) for p in pairs) if pairs else {}

    def __len__(self):
        return len(self.pairs)

    def __iter__(self) -> Iterator[Pair]:
        return iter(self.pairs.values())

    def __getitem__(self, symbols: Symbol | Iterable[Symbol]) -> Pair | Self:
        return (
            self.pairs[symbols]
            if isinstance(symbols, Symbol) else
            Pairs(self.pairs[x] for x in symbols)
        )

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join([x.base_symbol for x in self])})'

    def get_symbols(self) -> list[Symbol]:
        return list(self.pairs.keys())

    def get_base_symbols(self) -> list[Symbol]:
        return [x.base_symbol for x in self.pairs.values()]

    def get_sorted_by_turnover(self, ascending=False) -> list[Pair]:
        return sorted(self.pairs.values(), key=lambda x: x.turnover, reverse=not ascending)

    def extend(self, pairs: Pair | Iterable[Pair]):
        if isinstance(pairs, Pair): pairs = [pairs]
        self.pairs |= {x.symbol: x for x in pairs}

    def remove(self, symbols: Symbol | Iterable[Symbol]):
        if isinstance(symbols, Pair): symbols = [symbols]
        for x in symbols: self.pairs.pop(x)
