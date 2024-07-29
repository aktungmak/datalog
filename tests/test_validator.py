import unittest

import parser
import validator


class NonRecursiveValidatorTests(unittest.TestCase):
    def _parse_and_validate(self, program_string: str) -> list[validator.ValidationError]:
        prog = parser.parse_string(program_string)
        return list(validator.NonRecursiveValidator(prog).validate())

    def test_all_facts_ground(self):
        errors = self._parse_and_validate("""fact1(5, variable).""")
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], validator.NonGroundFactArgument)

    def test_range_restriction(self):
        errors = self._parse_and_validate("""rule1(a, b): fact1(b). fact1(5).""")
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], validator.RangeRestriction)

    def test_undefined_premise(self):
        errors = self._parse_and_validate("""rule1(a, b): undefined(a, b).""")
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], validator.UndefinedPremise)

    def test_recursive_rules(self):
        errors = self._parse_and_validate(
            """rule1(a): rule2(a).
               rule2(b): rule3(b).
               rule3(c): rule1(c).
            """)
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], validator.RecursiveRules)

    def test_self_recursive_rule(self):
        errors = self._parse_and_validate(
            """rule1(a): rule1(a).""")
        self.assertEqual(1, len(errors))
        self.assertIsInstance(errors[0], validator.RecursiveRules)
