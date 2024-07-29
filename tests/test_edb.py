import unittest

import pandas as pd

from dast import VariableTerm, NumberTerm
from edb import PandasRelation


class PandasRelationTests(unittest.TestCase):
    df1 = pd.DataFrame({"A": [1, 2, 3], "B": [11, 22, 33], "C": [111, 222, 333]})
    r1 = PandasRelation(df1)
    df2 = pd.DataFrame({"A": [1, 2, 6], "B": [11, 66, 33], "D": [5, 6, 7]})
    r2 = PandasRelation(df2)

    def test_select(self):
        r = self.r1.select([VariableTerm("a"), NumberTerm(11), VariableTerm("b")])
        self.assertEqual(1, len(r))

    def test_select_no_results(self):
        r = self.r1.select([VariableTerm("a"), NumberTerm(300), VariableTerm("b")])
        self.assertEqual(0, len(r))

    def test_select_incorrect_column_count(self):
        self.assertRaises(AssertionError, self.r1.select, [VariableTerm("a")])

    def test_project(self):
        names = ["A", "B"]
        r = self.r1.project(names)
        self.assertEqual(names, r.column_names)

    def test_project_unknown_names(self):
        names = ["C", "B"]
        r = self.r1.project(names + ["extra"])
        self.assertEqual(names, r.column_names)

    def test_rename(self):
        names = ["Q", "X", "Y"]
        r = self.r1.rename(names)
        self.assertEqual(r.column_names, names)

    def test_rename_incorrect_number_names(self):
        self.assertRaises(ValueError, self.r1.rename, ["Q", "X"])

    def test_join(self):
        r = self.r1.join(self.r2)
        self.assertEqual(1, len(r))

    def test_union(self):
        r = self.r1.union(self.r2)
        self.assertEqual(6, len(r))
