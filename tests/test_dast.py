import unittest

import parser
from dast import Atom, VariableTerm, NumberTerm


class ProgramTests(unittest.TestCase):
    prog = parser.parse_string("""a(A, B): p1(A, B).
                                  a(1, B): p2(A, B).
                                  a(C):    p3(C).
                                  b(Q, M): p4(Q, M).""")

    def test_unify(self):
        atom = parser.parse_string("a(X, Y)?")
        result = self.prog.unify(atom)
        self.assertEqual(2, len(list(result)))

    def test_unify_with_constant(self):
        atom = parser.parse_string("a(1, Y)?")
        result = self.prog.unify(atom)
        self.assertEqual(2, len(list(result)))

    def test_unify_with_constant_no_match(self):
        atom = parser.parse_string("a(2, Y)?")
        result = self.prog.unify(atom)
        self.assertEqual(1, len(list(result)))

    def test_unify_no_match(self):
        atom = parser.parse_string("c(N, M)?")
        result = self.prog.unify(atom)
        self.assertEqual(0, len(list(result)))


class AtomTests(unittest.TestCase):
    a = Atom(pred_sym="j",
             args=[VariableTerm("m"), NumberTerm(88), VariableTerm("s")])

    def test_position_to_name(self):
        self.assertEqual({0: "m", 2: "s"}, self.a.position_to_name)

    def test_name_to_position(self):
        self.assertEqual({"m": 0, "s": 2}, self.a.name_to_position)
