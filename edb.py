from abc import ABC, abstractmethod
from functools import reduce
from operator import and_
from typing import Self, Optional

import pandas as pd

from dast import Atom, VariableTerm, ConstantTerm, Term


class Relation(ABC):
    @classmethod
    @abstractmethod
    def single_row(cls, **cols) -> Self: pass

    @property
    @abstractmethod
    def column_names(self) -> list[str]: pass

    @abstractmethod
    def select(self, query: Atom) -> Self: pass

    @abstractmethod
    def project(self, names: list[str]) -> Self: pass

    @abstractmethod
    def rename(self, new_names: list[str]) -> Self: pass

    @abstractmethod
    def join(self, other: Self) -> Self: pass

    @abstractmethod
    def union(self, other: Optional[Self]) -> Self: pass


class EDB(ABC):
    def unify(self, query: Atom) -> Optional[Relation]:
        rel = self.match_relation(query)
        if rel is not None:
            return rel.select(query).project([arg.name for arg in query.args if isinstance(arg, VariableTerm)])

    @abstractmethod
    def match_relation(self, query: Atom) -> Optional[Relation]:
        pass


class PandasRelation(Relation):
    def __init__(self, df: pd.DataFrame):
        self._df = df

    @classmethod
    def single_row(cls, **cols):
        df = pd.DataFrame([cols])
        return PandasRelation(df)

    @property
    def column_names(self) -> list[str]:
        return list(self._df.columns)

    def select(self, terms: list[Term]) -> Self:
        df = self._df
        assert len(terms) == self._df.shape[1]
        conditions = [df.iloc[:, i] == term.value
                      for i, term in enumerate(terms)
                      if isinstance(term, ConstantTerm)]
        if len(conditions):
            df = df[reduce(and_, conditions)]
        else:
            df = df.iloc[:0]
        return PandasRelation(df)

    def project(self, names: list[str]) -> Self:
        return PandasRelation(self._df.filter(names))

    def rename(self, new_names: list[str]) -> Self:
        if len(new_names) != len(self._df.columns):
            raise ValueError(f"{len(new_names)} names provided but relation has {len(self._df.columns)} columns")
        return PandasRelation(self._df.set_axis(new_names, axis=1))

    def join(self, other: Self) -> Self:
        return PandasRelation(self._df.merge(other._df))

    def union(*relations: Self) -> Self:
        return PandasRelation(pd.concat([rel._df for rel in relations]))

    def __len__(self):
        return len(self._df)

    def __repr__(self):
        return f"<PandasRelation({', '.join(self._df.columns)}) with {self._df.shape[0]} rows"


class PandasEDB(EDB):
    def __init__(self, dataframes: dict[tuple[str, int], list[pd.DataFrame]] = None):
        self._dfs = {k: [PandasRelation(df) for df in dfs] for k, dfs in dataframes.items()}

    def match_relations(self, pred_sym: str, arity: int) -> list[Relation]:
        return self._dfs.get((pred_sym, arity), [])
