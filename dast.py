from collections.abc import Iterator
from dataclasses import dataclass, field
from numbers import Number
from typing import Any

import networkx as nx


@dataclass
class Node:
    """common superclass for all AST nodes"""
    location: dict = field(default_factory=dict, kw_only=True,
                           repr=False, compare=False)


@dataclass
class Term(Node): pass


@dataclass
class VariableTerm(Term):
    name: str
    value = None


@dataclass
class ConstantTerm(Term):
    value: Any


@dataclass
class StringTerm(ConstantTerm):
    value: str


@dataclass
class NumberTerm(ConstantTerm):
    value: Number


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

    @property
    def position_to_name(self) -> dict[int, str]:
        return {i: arg.name
                for i, arg in enumerate(self.args)
                if isinstance(arg, VariableTerm)}

    @property
    def name_to_position(self) -> dict[str, int]:
        return {v: k for k, v in self.position_to_name.items()}


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
class Fact(Clause): pass


@dataclass
class Rule(Clause):
    premises: list[Atom]


@dataclass
class Program(Node):
    clauses: list[Clause]

    def __post_init__(self):
        self.dependency_graph = nx.DiGraph()
        for rule in self.rules:
            src_key = rule.pred_sym, rule.arity
            self.dependency_graph.add_node(src_key, node=rule)
            for premise in rule.premises:
                dst_key = premise.pred_sym, premise.arity
                self.dependency_graph.add_edge(src_key, dst_key)

    @property
    def facts(self) -> list[Fact]:
        return [c for c in self.clauses if isinstance(c, Fact)]

    @property
    def rules(self) -> list[Rule]:
        return [c for c in self.clauses if isinstance(c, Rule)]

    def unify(self, atom: Atom) -> Iterator[Rule]:
        for rule in self.rules:
            if (atom.pred_sym == rule.pred_sym
                    and atom.arity == rule.arity
                    and all(isinstance(atom_arg, VariableTerm)
                            or isinstance(rule_arg, VariableTerm)
                            or atom_arg.value == rule_arg.value
                            for atom_arg, rule_arg in zip(atom.args, rule.head.args))):
                yield rule
