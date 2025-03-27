from typing import Iterable, Iterator, KeysView

from src.pair import Pair, Symbol


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
        return f'{self.__class__.__name__}({", ".join([x.pretty_symbol for x in self])})'

    def update(self, pairs: Iterable[Pair]):
        self.pairs |= {x.symbol: x for x in pairs}

    def get_symbols(self) -> KeysView[Symbol]:
        return list(self.pairs.keys())
