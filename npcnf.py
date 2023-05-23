import numpy as np


def sign(x): return (1, -1)[x < 0]


def parse(filename: str):
    lines = open(filename).readlines()

    clauses = ""
    for line in lines:
        if line.startswith("c"):
            continue
        if line.startswith("p"):
            _, _, num_variables, num_clauses = line.split()
            num_variables = int(num_variables)
            num_clauses = int(num_clauses)
        else:
            clauses += line

    formula = np.zeros((num_clauses, num_variables))
    for clause_num, clause in enumerate(clauses.split(" 0")):
        for v in clause.split():
            v = int(v)
            idx = abs(v) - 1
            formula[clause_num, idx] = sign(v)
    return formula


def apply_solution(formula, solution):
    a = np.count_nonzero(formula, axis=1)
    b = formula @ solution
    return b == -a


def satisfies(formula, solution):
    return not any(apply_solution(formula, solution))


# interestingly, complementary solutions do not give complementary results
def gen_complementary_solutions(indices, length):
    neg = np.zeros(length)
    pos = np.ones(length)
    neg[indices,] = 1
    pos[indices,] = 0
    return neg, pos
