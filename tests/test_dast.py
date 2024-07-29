import unittest

import parser


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
