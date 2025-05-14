import unittest

import parser
import validator
from edb import PandasEDB
from evaluator import BaseEvaluator


class NonRecursiveEvaluatorTests(unittest.TestCase):
    prog = parser.parse_string("""
        r1(A, B): f1(A), r2(A, B).
        r2(M, N): f2(M), r3(M, N).
        r3(X, Y): f3(X), f4(Y).
        
        f1(11).
        f1(12).
        f2(21).
        f2(22).
        f3(31).
        f3(32).
        f4(41).
        f4(42).
    """)
    prog = parser.parse_string("""
        f1(11).
        f1(12).
    """)
    query = parser.parse_string("r1(J, G)?")
    query = parser.parse_string("f1(J)?")
    def test_evaluate(self):
        # errors = validator.NonRecursiveValidator(self.prog).validate()
        # print(list(errors))
        edb = PandasEDB()
        bev = BaseEvaluator(self.prog, edb)
        rel = bev.query(self.query)
        print(rel._df)

