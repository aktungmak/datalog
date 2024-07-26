from collections import defaultdict
from functools import reduce
from itertools import combinations
from operator import and_
from typing import Callable, Any

import pandas as pd

import dast

FactTableMapping = dict[tuple[str, int], list[pd.DataFrame]]


class PandasEvaluator:
    def __init__(self, program: dast.Program, fact_tables: FactTableMapping = []):
        self.prog = program
        self.fact_tables = defaultdict(list, fact_tables)

        for fact_key, facts in self.prog.facts.items():
            df = pd.DataFrame([arg.value for fact in facts for arg in fact.args])
            self.fact_tables[fact_key].append(df)

    def query(self, query: str) -> list[dast.Atom]:
        qa = dast.Atom.parse_string(query)
        result = pd.DataFrame()

        candidate_facts = self.fact_tables[qa.pred_sym, qa.arity]
        for candidate in candidate_facts:
            result = pd.concat([result, unify_facts(candidate, qa.args)])
            return result

        candidate_rules = self.prog.rules[qa.pred_sym, qa.arity]
        for candidate in candidate_rules:
            _, bindings = partition(is_var, enumerate(qa.args))
            candidate


def unify_facts(df: pd.DataFrame, terms: list[dast.Term]):
    var_terms, ground_terms = partition(is_var, enumerate(terms))
    var_conditions = [df[i] == df[j] for (i, t1), (j, t2) in combinations(var_terms, 2) if t1.name == t2.name]
    ground_conditions = [df[i] == term.value for i, term in ground_terms]
    conditions = ground_conditions + var_conditions
    if conditions:
        return df[reduce(and_, conditions)]
    else:
        return df




def is_var(term: dast.Term):
    return isinstance(term, dast.VariableTerm)


def partition(condition: Callable[[Any], bool], items: iter) -> tuple[list, list]:
    true = []
    false = []
    for item in items:
        if condition(item):
            true.append(item)
        else:
            false.append(item)
    return true, false
