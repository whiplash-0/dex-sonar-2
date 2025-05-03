from typing import Iterable, Iterator, KeysView

from src.pairs.pair import Pair, Symbol


class Pairs:
    def __init__(self):
        self.pairs: dict[Symbol, Pair] = {}

    def __len__(self):
        return len(self.pairs)

    def __iter__(self) -> Iterator[Pair]:
        return iter(self.pairs.values())

    def __getitem__(self, symbol: Symbol) -> Pair:
        return self.pairs[symbol]

    def __repr__(self):
        return f'{self.__class__.__name__}({", ".join([x.base_symbol for x in self])})'

    def get_symbols(self) -> KeysView[Symbol]:
        return self.pairs.keys()

    def get_sorted_by_turnover(self, ascending=False) -> list[Pair]:
        return sorted(self.pairs.values(), key=lambda x: x.turnover, reverse=not ascending)

    def update(self, pairs: Pair | Iterable[Pair]):
        if isinstance(pairs, Pair): pairs = [pairs]
        self.pairs |= {x.symbol: x for x in pairs}
