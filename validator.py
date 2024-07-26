import itertools
from collections.abc import Iterator

import networkx as nx

from dast import Program


class ValidationError(Exception):
    def __init__(self, message, term=None):
        self.message = message
        self.term = term
        super().__init__(self.message)


class NonRecursiveValidator:
    def __init__(self, program: Program):
        self.program = program

    def validate(self) -> Iterator[ValidationError]:
        return itertools.chain(self.all_fact_args_ground(),
                               self.no_undefined_premise(),
                               self.is_dag())

    def all_fact_args_ground(self) -> list[ValidationError]:
        for fact in self.program.facts:
            for variable in fact.head.variables:
                yield ValidationError(f"non-ground argument in fact {fact.head.pred_sym}", variable)

    def no_undefined_premise(self) -> Iterator[ValidationError]:
        clause_keys = [(c.pred_sym, c.arity) for c in self.program.clauses]
        for rule in self.program.rules:
            for premise in rule.premises:
                if (premise.pred_sym, premise.arity) not in clause_keys:
                    yield ValidationError("undefined premise", premise)

    def is_dag(self) -> Iterator[ValidationError]:
        try:
            cycle = nx.find_cycle(self.program.dependency_graph)
            yield ValidationError(f"dependency graph has a cycle: {cycle}")
        except nx.NetworkXNoCycle:
            return
