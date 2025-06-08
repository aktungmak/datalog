import unittest

import pandas as pd

from dast import VariableTerm, NumberTerm, Atom, ConstantTerm
from edb import PandasRelation, PandasEDB

class PandasEDBTests(unittest.TestCase):
    df1 = pd.DataFrame({"A": [1, 2, 3], "B": [11, 22, 33]})
    r1 = PandasRelation(df1)
    edb = PandasEDB()
    rel_name = "r1"
    edb.add_relation(rel_name, r1)

    def test_unify_general(self):
        r = self.edb.unify(Atom(self.rel_name, [VariableTerm("X"), VariableTerm("Y")]))
        self.assertEqual(len(self.r1), len(r))
        self.assertEqual(self.r1.column_names, r.column_names)
    def test_unify_specific(self):
        r = self.edb.unify(Atom(self.rel_name, [ConstantTerm(2), VariableTerm("Y")]))

        self.assertEqual(["Y"], r.column_names)
        self.assertEqual(1, len(r))

    def test_unify_no_match(self):
        r = self.edb.unify(Atom(self.rel_name, [VariableTerm("X")]))
        self.assertEqual(0, len(r))

class PandasRelationTests(unittest.TestCase):
    df1 = pd.DataFrame({"A": [1, 2, 3], "B": [11, 22, 33], "C": [111, 222, 333]})
    r1 = PandasRelation(df1)
    df2 = pd.DataFrame({"A": [1, 2, 6], "B": [11, 66, 33], "D": [5, 6, 7]})
    r2 = PandasRelation(df2)

    def test_select(self):
        r = self.r1.select(Atom("a", [VariableTerm("a"), NumberTerm(11), VariableTerm("b")]))
        self.assertEqual(1, len(r))
        self.assertEqual(["a", "b"], r.column_names)

    def test_select_no_results(self):
        r = self.r1.select(Atom("a", [VariableTerm("a"), NumberTerm(300), VariableTerm("b")]))
        self.assertEqual(0, len(r))
        self.assertEqual(["a", "b"], r.column_names)

    def test_select_incorrect_column_count(self):
        r = self.r1.select(Atom("a", [VariableTerm("a")]))
        self.assertEqual(0, len(r))

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
