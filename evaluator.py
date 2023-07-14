from functools import reduce

import networkx as nx
import pyspark
import pyspark.sql.types as st
from pyspark.sql import DataFrame


def multiple_join(dfs: list[DataFrame]) -> DataFrame:
    def join(left, right):
        on = list(set(left.columns) & set(right.columns))
        return left.join(right, on=on, how='inner')

    return reduce(join, dfs)


class ExecutionError(Exception):
    def __init__(self, message, tok):
        super().__init__(self.message)


class SparkDatalog:
    def __init__(self, spark: pyspark.sql.SparkSession):
        self.spark = spark

    def premise_to_df(self, premise: Premise) -> DataFrame:
        df = self.spark.read.table(premise.pred_sym)
        if len(df.columns) != len(premise.args):
            raise ExecutionError(f"mismatched arity, expected {premise.args} got {df.columns}")

        for arg, col in zip(premise.args, df.columns):
            if isinstance(arg, VariableTerm):
                df = df.withColumnRenamed(col, arg.name)
            else:
                df = df.filter(df[col] == arg.value)

    def rule_to_df(rule: Rule, spark) -> DataFrame:
        dfs = [premise_to_df(premise) for premise in rule.body]
        df = multiple_join(dfs)

    def rule_to_schema(rule: Rule):
        fields = []
        for arg in rule.head.args:
            if isinstance(arg, StringTerm):
                fields.append(st.StringType)
            elif isinstance(arg, NumberTerm):
                fields.append(st.NumericType)
            else:
                raise ExecutionError(f"unknown type {type(arg)}")
        return st.StructType(fields)


def eval_nonrecursive(p: Program, q: Atom) -> DataFrame:
    # validate not recursive
    p.validate_nonrecursive()
    # build query tree bottom up

    for clause in reversed(nx.topological_sort(p.dependency_graph)):
        df = clause_to_df(clause)


def to_pyspark(self, program: Program, query: Atom):
    for rule in program.rules:
        # register a placeholder view
        # we could just add these to a dict along with references to edb tables
        self.spark.createDataFrame([], rule_to_schema(rule)).createOrReplaceTempView(f"{rule.pred_sym}_{rule.arity}")
    rules = [rule_to_df(rule) for rule in program.rules]
