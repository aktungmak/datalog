import itertools
from collections.abc import Iterator
from dataclasses import dataclass

import networkx as nx

import dast
from dast import Program


@dataclass
class ValidationError(Exception):
    message: str
    node: dast.Node = None


class NonGroundFactArgument(ValidationError):
    """All arguments of a fact should be ground"""

class RangeRestriction(ValidationError):
    """All variables in a rule head must also appear in the body"""

class UndefinedPremise(ValidationError):
    """All premises in the body of the rule must be defined"""

class RecursiveRules(ValidationError):
    """Rules may not depend on each other recursively"""


class NonRecursiveValidator:
    def __init__(self, program: Program):
        self.program = program

    def validate(self) -> Iterator[ValidationError]:
        return itertools.chain(self._all_fact_args_ground(),
                               self._range_restriction(),
                               self._undefined_premise(),
                               self._recursive_rules())

    def _all_fact_args_ground(self) -> Iterator[ValidationError]:
        for fact in self.program.facts:
            for variable in fact.head.variables:
                yield NonGroundFactArgument(f"non-ground argument in fact {fact.head.pred_sym}", variable)

    def _range_restriction(self) -> Iterator[ValidationError]:
        for rule in self.program.rules:
            for arg in rule.head.args:
                for premise in rule.premises:
                    if arg in premise.args:
                        break
                else:
                    yield RangeRestriction(f"{arg} not restricted in rule {rule.pred_sym}/{rule.arity}", arg)


    def _undefined_premise(self) -> Iterator[ValidationError]:
        clause_keys = {(c.pred_sym, c.arity) for c in self.program.clauses}
        for rule in self.program.rules:
            for premise in rule.premises:
                if (premise.pred_sym, premise.arity) not in clause_keys:
                    yield UndefinedPremise("undefined premise", premise)

    def _recursive_rules(self) -> Iterator[ValidationError]:
        try:
            cycle = nx.find_cycle(self.program.dependency_graph)
            cycle_str = " -> ".join([f"{step[0][0]}/{step[0][1]}" for step in cycle])

            yield RecursiveRules(f"dependency graph has a cycle: {cycle_str}")
        except nx.NetworkXNoCycle:
            return
