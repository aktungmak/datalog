from enum import Enum, auto
from typing import Optional

from dast import Program, Atom
from edb import EDB, Relation


class Features(Enum):
    Recursion = auto()
    Negation = auto()


class BaseEvaluator:
    supported_features = set()

    def __init__(self, program: Program, edb: EDB):
        self.program = program
        self.edb = edb

        edb.insert_facts(program.facts)

    # TODO change from recursion to iteration
    def query(self, atom: Atom, input_rel: Optional[Relation] = None) -> Relation:
        query_rel = self.edb.atom_as_relation(atom)
        if input_rel is None:
            input_rel = query_rel
        else:
            input_rel = input_rel.cross(query_rel)

        # facts
        yield from self.edb.unify(atom)

        # rules
        for candidate_rule in self.program.unify(atom):
            supp_rel = input_rel.rename(candidate_rule.head.position_to_name)
            for premise in candidate_rule.premises:
                rel = self.query(premise, supp_rel.rename(premise.name_to_position))
                supp_rel = supp_rel.join(rel)

            distinguished = [var.name for var in candidate_rule.head.variables]
            output = output.union(supp_rel.project(distinguished)) # may need to name->pos here

        output_names = list(atom.name_to_position.keys())
        return output.project(output_names)


"""
r1(A, B, C):
    r2(A, 1, D),
    r3(B, 2, E),
    r4(C, 3, F).
"""
