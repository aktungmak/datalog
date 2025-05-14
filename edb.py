from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from functools import reduce
from operator import and_
from typing import Self, Optional

import pandas as pd

from dast import Atom, VariableTerm, ConstantTerm, Term, Fact


class Relation(ABC):
    @property
    @abstractmethod
    def column_names(self) -> list[str]: pass

    @abstractmethod
    def select(self, query: Atom) -> Self: pass

    @abstractmethod
    def project(self, names: list[str]) -> Self: pass

    @abstractmethod
    def rename(self, new_names: dict[str, str]) -> Self: pass

    @abstractmethod
    def join(self, other: Optional[Self]) -> Self: pass

    @abstractmethod
    def union(self, *others: Self) -> Self: pass

    @abstractmethod
    def cross(self, other: Optional[Self]) -> Self: pass


class EDB(ABC):
    def unify(self, query: Atom) -> Relation:
        for rel in self.get_relations(query):
            return rel.select(query).project([arg.name for arg in query.args
                                              if isinstance(arg, VariableTerm)])

    @staticmethod
    def atom_as_relation(atom: Atom) -> Relation: pass

    @abstractmethod
    def get_relations(self, atom: Atom) -> Iterator[Relation]: pass

    @abstractmethod
    def add_relation(self, atom: Atom, relation: Relation): pass

    @abstractmethod
    def insert_facts(self, facts: Iterable[Fact]): pass


class PandasRelation(Relation):
    def __init__(self, df: pd.DataFrame):
        self._df = df

    @property
    def column_names(self) -> list[str]:
        return list(self._df.columns)

    def select(self, query: Atom) -> Self:
        df = self._df
        conditions = [df.iloc[:, i] == term.value
                      for i, term in enumerate(query.args)
                      if isinstance(term, ConstantTerm)]
        print(conditions)
        if len(conditions):
            return PandasRelation(df[reduce(and_, conditions)])
        else:
            return self

    def project(self, names: Iterable[str]) -> Self:
        return PandasRelation(self._df.filter(names))

    def rename(self, new_names: dict[str, str]) -> Self:
        return PandasRelation(self._df.rename(new_names))

    def join(self, other: Optional[Self]) -> Self:
        if other is None:
            return self
        else:
            return PandasRelation(self._df.merge(other._df))

    def union(*relations: Self) -> Self:
        return PandasRelation(pd.concat([rel._df for rel in relations]))

    def cross(self, other: Optional[Self]) -> Self:
        if other is None:
            return self
        else:
            return PandasRelation(self._df.merge(other._df, how="cross"))

    def __len__(self):
        return len(self._df)

    def __repr__(self):
        return f"<PandasRelation({', '.join(self._df.columns)})> with {len(self)} rows"


class PandasEDB(EDB):
    def __init__(self, dataframes: dict[tuple[str, int], pd.DataFrame] = None):
        if dataframes is None:
            self._dfs = {}
        else:
            self._dfs = {k: PandasRelation(df)
                         for k, df in dataframes.items()}

    @staticmethod
    def atom_as_relation(atom: Atom) -> PandasRelation:
        row = (arg.value for arg in atom.args if isinstance(arg, ConstantTerm))
        df = pd.DataFrame([row])
        return PandasRelation(df)

    def get_relation(self, atom: Atom) -> Optional[PandasRelation]:
        return self._dfs.get((atom.pred_sym, atom.arity))

    def add_relation(self, atom: Atom, relation: PandasRelation):
        self._dfs[(atom.pred_sym, atom.arity)] = relation

    # TODO batch together facts with same signature
    def insert_facts(self, facts: Iterable[Fact]):
        for fact in facts:
            new_rel = self.atom_as_relation(fact.head)
            old_rel = self.get_relation(fact.head)
            if old_rel is not None:
                new_rel = new_rel.union(old_rel)
            self.add_relation(fact.head, new_rel)
