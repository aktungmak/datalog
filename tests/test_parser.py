import tokenize
import unittest

from dast import Program, Fact, Atom, VariableTerm, StringTerm, NumberTerm, Rule
import parser


class PositiveParserTests(unittest.TestCase):
    fact_str = """fact1(a, B, 'single', "double", 22, 3.14)."""
    expected_fact = Fact(head=Atom(pred_sym="fact1",
                                   args=[VariableTerm(name="a"),
                                         VariableTerm(name="B"),
                                         StringTerm(value="single"),
                                         StringTerm(value="double"),
                                         NumberTerm(value=22),
                                         NumberTerm(value=3.14)
                                         ]))
    rule_str = """rule1(a, B, c):
                      premise1(a, B, 1),
                      premise2(B, c, 2)."""
    expected_rule = Rule(head=Atom(pred_sym='rule1',
                                   args=[VariableTerm(name='a'),
                                         VariableTerm(name='B'),
                                         VariableTerm(name='c')]),
                         premises=[Atom(pred_sym='premise1',
                                        args=[VariableTerm(name='a'),
                                              VariableTerm(name='B'),
                                              NumberTerm(value=1)]),
                                   Atom(pred_sym='premise2',
                                        args=[VariableTerm(name='B'),
                                              VariableTerm(name='c'),
                                              NumberTerm(value=2)])])

    def test_one_fact(self):
        prog = parser.parse_string(self.fact_str)
        expected = Program(clauses=[self.expected_fact])
        self.assertEqual(prog, expected)

    def test_one_rule(self):
        prog = parser.parse_string(self.rule_str)
        expected = Program(clauses=[self.expected_rule])
        self.assertEqual(prog, expected)

    def test_rule_and_fact(self):
        prog = parser.parse_string(self.rule_str + self.fact_str)
        expected = Program(clauses=[self.expected_rule, self.expected_fact])
        self.assertEqual(prog, expected)


@unittest.skip
class NegativeParserTests(unittest.TestCase):
    def test_unmatched_paren(self):
        prog = """bad_fact(1, 2,"""
        self.assertRaises(tokenize.TokenError, parser.parse_string, prog)

    def test_missing_colon(self):
        prog = """bad_rule(a, b) premise1(a, b)"""
        self.assertRaises(tokenize.TokenError, parser.parse_string, prog)


if __name__ == '__main__':
    unittest.main()
