import token
from collections import deque, defaultdict
from functools import reduce
from io import StringIO
from tokenize import generate_tokens, TokenInfo
from typing import Optional

import networkx as nx
import pyspark
from pyspark.sql import DataFrame
import pyspark.sql.types as st


class ParseError(Exception):
    def __init__(self, message, tok):
        location = f" on line {tok.start[0]}, pos {tok.start[1]}"
        caret = " " * tok.start[1] + "^"
        self.message = "\n".join([message + location, tok.line, caret])
        self.tok = tok
        super().__init__(self.message)


class ValidationError(Exception):
    def __init__(self, message, tok):
        self.message = message
        self.tok = tok
        super().__init__(self.message)


class Tokenizer:
    ignored_token_types = [token.NEWLINE, token.INDENT]

    def __init__(self, datalog_string: str):
        self._gen = generate_tokens(StringIO(datalog_string).readline)
        self._queue = deque()
        self.last = None

    def consume(self, expected: str = None) -> TokenInfo:
        if self._queue:
            self.last = self._queue.popleft()
        else:
            while True:
                self.last = next(self._gen)
                if self.last.type in self.ignored_token_types:
                    continue
                else:
                    break

        if expected is not None and expected != self.last.string:
            raise ParseError(
                f"expected {expected}, found {self.last.string}", self.last
            )

        # print(f"consume {expected} {self.last}")
        return self.last

    def try_consume(self, expected: str, expected_type: Optional[int] = None) -> bool:
        tok = self.consume()
        # print(f"try_consume {expected} {tok}")
        if tok.string == expected or tok.type == expected_type:
            return True
        else:
            self._queue.append(tok)
            return False


class Term:
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer):
        tok = tokenizer.consume()
        if tok.type == token.NAME and tok.string[0].isupper():
            return VariableTerm(tok)
        elif tok.type == token.NAME and tok.string[0].islower():
            return StringTerm(tok)
        elif tok.type == token.STRING:
            return StringTerm(tok)
        elif tok.type == token.NUMBER:
            return NumberTerm(tok)
        else:
            raise ParseError("invalid term", tok)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer):
        while True:
            if tokenizer.try_consume(")"):
                break
            yield Term.parse_one(tokenizer)
            tokenizer.try_consume(",")


class VariableTerm(Term):
    def __init__(self, tok: TokenInfo, value=None):
        self.tok = tok
        self.name = tok.string
        self.value = value

    def __eq__(self, other) -> bool:
        return isinstance(other, VariableTerm) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"VariableTerm(name={self.name}, value={self.value})"


class StringTerm(Term):
    def __init__(self, tok: TokenInfo):
        self.tok = tok
        self.value = tok.string.strip('"')

    def __repr__(self) -> str:
        return f"StringTerm(value={self.value})"


class NumberTerm(Term):
    def __init__(self, tok: TokenInfo):
        self.tok = tok
        self.value = float(tok.string)

    def __repr__(self) -> str:
        return f"NumberTerm(value={self.value})"


class Atom:
    @classmethod
    def parse(cls, tokenizer: Tokenizer, negated: bool = False):
        pred_sym_token = tokenizer.consume()
        tokenizer.consume("(")
        args = list(Term.parse_all(tokenizer))
        return Atom(pred_sym_token, args, negated=negated)

    def __init__(self, pred_sym_token: TokenInfo, args: list[Term], negated=False):
        self._pred_sym_token = pred_sym_token
        self.pred_sym = pred_sym_token.string
        self.args = args
        self.arity = len(args)
        self.negated = negated
        self.vars = [t for t in self.args if isinstance(t, VariableTerm)]

    def __repr__(self) -> str:
        return f"Atom(pred_sym={self.pred_sym}, args={self.args}, negated={self.negated})"


class Premise(Atom):
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer) -> Atom:
        negated = tokenizer.try_consume("~")
        return Atom.parse(tokenizer, negated=negated)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer):
        while True:
            yield Premise.parse_one(tokenizer)
            if tokenizer.try_consume(","):
                continue
            else:
                tokenizer.consume(".")
                break


class Clause:
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer):
        head = Atom.parse(tokenizer)

        if tokenizer.try_consume("."):
            return Fact(head)
        elif tokenizer.try_consume(":"):
            body = [p for p in Premise.parse_all(tokenizer)]
            return Rule(head, body)
        else:
            tok = tokenizer.consume()
            raise ParseError(f"invalid terminator", tok)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer):
        while True:
            yield Clause.parse_one(tokenizer)
            if tokenizer.try_consume("", expected_type=token.ENDMARKER):
                break


class Fact(Clause):
    def __init__(self, atom: Atom):
        self._atom = atom
        self.pred_sym = atom.pred_sym
        self.arity = atom.arity
        self.arg_values = [a.value for a in atom.args]

    def validate(self) -> list[ValidationError]:
        return [
            ValidationError(f"non-ground arg '{var.tok.string}' in Fact", var.tok)
            for var in self._atom.vars
        ]

    def __repr__(self) -> str:
        return f"Fact(pred_sym={self.pred_sym}, args={self._atom.args})"


class Rule(Clause):
    def __init__(self, head: Atom, body: list[Premise]):
        self.head = head
        self.body = body
        self.pred_sym = head.pred_sym
        self.arity = head.arity

    def validate(self) -> list[ValidationError]:
        return self.validate_range_restriction() + self.validate_negation_safety()

    def validate_range_restriction(self) -> list[ValidationError]:
        head_vars = set(self.head.vars)
        body_vars = {v for p in self.body for v in p.vars}
        unrestricted_vars = head_vars - body_vars
        return [ValidationError(f"unrestricted head var '{v.name}'", v) for v in unrestricted_vars]

    def validate_negation_safety(self) -> list[ValidationError]:
        pos_vars = {v for p in self.body for v in p.vars if not p.negated}
        neg_vars = {v for p in self.body for v in p.vars if p.negated}
        unsafe_vars = neg_vars - pos_vars
        return [ValidationError(f"unsafe negated var '{v.name}'", v) for v in unsafe_vars]

    def __repr__(self) -> str:
        return f"Rule(head={self.head}, body={self.body})"


class Program:

    @classmethod
    def parse_string(cls, program: str):
        tokenizer = Tokenizer(program)
        return Program.parse(tokenizer)

    @classmethod
    def parse(cls, tokenizer: Tokenizer):
        clauses = [clause for clause in Clause.parse_all(tokenizer)]
        return Program(clauses)

    def __init__(self, clauses: list[Clause]):
        self.clauses = clauses
        self.facts = {(c.pred_sym, c.arity): c for c in self.clauses if isinstance(c, Fact)}
        # todo handle multiple occurrences of the same rule as union
        self.rules = {(c.pred_sym, c.arity): c for c in self.clauses if isinstance(c, Rule)}
        # grouped_facts = {}
        # for fact in self.facts:
        #     grouped_facts.setdefault((fact.pred_sym, fact.arity), []).append(fact.arg_values)
        self.dependency_graph = self._build_dependency_graph()
        self.strata = self._stratify()

    def get_rule(self, pred_sym: str, arity: int) -> Rule:
        return self.rules[(pred_sym, arity)]

    def _build_dependency_graph(self) -> nx.DiGraph:
        g = nx.DiGraph()
        for rule_key, rule in self.rules.items():
            for premise in rule.body:
                g.add_edge(rule_key, (premise.pred_sym, premise.arity))
        return g

    def _validate(self) -> list[ValidationError]:
        return sum([c.validate() for c in self.clauses], [])

    def validate_nonrecursive(self) -> list[ValidationError]:
        return sum([self._validate(), self._validate_is_dag()], [])

    def _validate_is_dag(self):
        if not nx.is_directed_acyclic_graph(self.dependency_graph):
            return [ValidationError('Dependency graph is not acyclic')]
        else:
            return []

    def __repr__(self) -> str:
        return f"Program({self.clauses})"

    def _stratify(self) -> list:
        # TODO stratify into a list of subprograms
        pass


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


s1 = 'a(1 2).a(1 4).a(5 6).d(X Y Z): ~a(1 Y).'
s2 = '''edge(a, b).
edge(b, c).
edge(c, d).
edge(c, c).
tc(X, Y) : edge(X, Y).
tc(X, Y) : tc(X, Z), tc(Z, Y).
'''
s3 = '''a(X, Y): b(X), c(Y).
b(X): d(X), e(X).
c(X): d(X), f(X).
'''

if __name__ == '__main__':
    p1 = Program.parse_string(s1)
    assert (len(p1.validate()) > 0)
    p2 = Program.parse_string(s2)
    assert (p2.validate() == [])
    print(p2.rules)
    print(p2.dependency_graph().adj_dict)
