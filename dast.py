from dataclasses import dataclass, field
from numbers import Number

import networkx as nx


@dataclass
class Node:
    """common superclass for all AST nodes"""
    location: dict = field(default_factory=dict, kw_only=True,
                           repr=False, compare=False)


@dataclass
class Term(Node):
    pass


@dataclass
class StringTerm(Term):
    value: str


@dataclass
class NumberTerm(Term):
    value: Number


@dataclass
class VariableTerm(Term):
    name: str
    value = None


@dataclass
class Atom(Node):
    pred_sym: str
    args: list[Term]

    @property
    def arity(self) -> int:
        return len(self.args)

    @property
    def variables(self) -> list[VariableTerm]:
        return [a for a in self.args if isinstance(a, VariableTerm)]


@dataclass
class Clause(Node):
    head: Atom

    @property
    def pred_sym(self) -> str:
        return self.head.pred_sym

    @property
    def arity(self) -> int:
        return self.head.arity


@dataclass
class Fact(Clause):
    pass


@dataclass
class Rule(Clause):
    premises: list[Atom]


@dataclass
class Program(Node):
    clauses: list[Clause]

    # def __post_init__(self):
    #     self.dependency_graph = nx.DiGraph()
    #     for rule in self.rules:
    #         src_key = rule.pred_sym, rule.arity
    #         for premise in rule.premises:
    #             dst_key = premise.pred_sym, premise.arity
    #             self.dependency_graph.add_edge(src_key, dst_key)

    @property
    def facts(self) -> list[Fact]:
        return [c for c in self.clauses if isinstance(c, Fact)]

    @property
    def rules(self) -> list[Rule]:
        return [c for c in self.clauses if isinstance(c, Rule)]
