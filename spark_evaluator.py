from functools import reduce

import pyspark
import pyspark.sql.types as st
from pyspark.sql import DataFrame

import dast as ast


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

    def premise_to_df(self, premise: ast.Premise) -> DataFrame:
        df = self.spark.read.table(premise.pred_sym)
        if len(df.columns) != len(premise.args):
            raise ExecutionError(f"mismatched arity, expected {premise.args} got {df.columns}")

        for arg, col in zip(premise.args, df.columns):
            if isinstance(arg, ast.VariableTerm):
                df = df.withColumnRenamed(col, arg.name)
            else:
                df = df.filter(df[col] == arg.value)

    def rule_to_df(rule: ast.Rule) -> DataFrame:
        dfs = [premise_to_df(premise) for premise in rule.body]
        df = multiple_join(dfs)

    def rule_to_schema(rule: ast.Rule):
        fields = []
        for arg in rule.head.args:
            if isinstance(arg, ast.StringTerm):
                fields.append(st.StringType)
            elif isinstance(arg, ast.NumberTerm):
                fields.append(st.NumericType)
            else:
                raise ExecutionError(f"unknown type {type(arg)}")
        return st.StructType(fields)


def eval_nonrecursive_topdown(q: ast.Atom, p: ast.Program, facts: dict={}) -> DataFrame:
    # validate not recursive
    p.validate_nonrecursive()

    qdf = query_atom_to_df(q)

    # first, match facts and add them to the result
    result = match_facts(qdf, facts)

    # match the query atom against facts and rules in the program
    # push them onto the stack to be considered
    stack = match_rules(q, p)
    # while the stack is full:
    #   pop the next atom
    #   if it is a fact

    while stack:
        x = stack.pop()

def query_atom_to_df(spark: pyspark.sql.SparkSession, q: ast.Atom) -> DataFrame:
    # create a dataframe containing the non-variable columns of q
    # named as the 0-based indices
    nonvar_args = [a for a in q.args if not isinstance(a, ast.VariableTerm)]
    return spark.createDataFrame([nonvar_args], range(len(nonvar_args)))


def match_facts(q: ast.Atom, qdf: DataFrame, facts:dict) -> list:
    result = []
    for f in facts[(q.pred_sym, q.arity)]:
        f.join(qdf)
                # this is equivalent to doing a semijoin on the nonvar columns of q
                for qa, fa in zip(q.args, c.args):
                    if isinstance(qa, ast.VariableTerm):
                        continue
                    if qa != fa:
                        break
                else:
                    result.append(c)

            # elif isinstance(c, ast.Rule):

    return result


s = "a(1,2).a(3,4).b(5 6)."
p = ast.Program.parse_string(s)
q = ast.Atom.parse_string("b(X, 6).")
print(match(p, q))


def to_pyspark(self, program: ast.Program, query: ast.Atom):
    for rule in program.rules:
        # register a placeholder view
        # we could just add these to a dict along with references to edb tables
        self.spark.createDataFrame([], rule_to_schema(rule)).createOrReplaceTempView(f"{rule.pred_sym}_{rule.arity}")
    rules = [rule_to_df(rule) for rule in program.rules]
