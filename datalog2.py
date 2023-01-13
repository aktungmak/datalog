import token
from collections import deque
from functools import reduce
from io import StringIO
from tokenize import generate_tokens, TokenInfo
from typing import Optional
import pandas as pd

DatabaseInstance = dict[tuple[str, int], pd.DataFrame]


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


class Premise:
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
            ValidationError("non-ground arg in Fact", var.tok)
            for var in self._atom.vars
        ]

    def __repr__(self) -> str:
        return f"Fact(pred_sym={self.pred_sym}, args={self._atom.args})"


class Rule(Clause):
    def __init__(self, head: Atom, body: list[Atom]):
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
        return [ValidationError("unrestricted head var", v) for v in unrestricted_vars]

    def validate_negation_safety(self) -> list[ValidationError]:
        pos_vars = {v for p in self.body for v in p.vars if not p.negated}
        neg_vars = {v for p in self.body for v in p.vars if p.negated}
        unsafe_vars = neg_vars - pos_vars
        return [ValidationError("unsafe negated var", v) for v in unsafe_vars]

    def __repr__(self) -> str:
        return f"Rule(head={self.head}, body={self.body})"


class Program:

    @classmethod
    def parse(cls, tokenizer: Tokenizer):
        clauses = [clause for clause in Clause.parse_all(tokenizer)]
        return Program(clauses)

    def __init__(self, clauses: list[Clause]):
        self.clauses = clauses
        self.facts = [c for c in self.clauses if isinstance(c, Fact)]
        self.rules = [c for c in self.clauses if isinstance(c, Rule)]
        grouped_facts = {}
        for fact in self.facts:
            grouped_facts.setdefault((fact.pred_sym, fact.arity), []).append(fact.arg_values)
        self.edb = {key: pd.DataFrame(rows) for key, rows in grouped_facts.items()}

    def validate(self) -> list[ValidationError]:
        return sum([c.validate() for c in self.clauses], [])

    def __repr__(self) -> str:
        return f"Program({self.clauses})"

    def stratify(self) -> list:
        # TODO stratify into a list of subprograms
        pass


def immcon(dbi: DatabaseInstance, rule: Rule) -> DatabaseInstance:
    tables = []
    for p in rule.body:
        df = dbi[(p.pred_sym, p.arity)]
        df.columns = [t.name if isinstance(t, VariableTerm) else hash(t) for t in p.args]
        for col, t in zip(df.columns, p.args):
            if not isinstance(t, VariableTerm):
                df = df[df[col] == t.value]
        tables.append(df)
    dbi[rule.pred_sym, rule.arity] = reduce(pd.DataFrame.merge, tables)
    # project result
    return dbi


def parse(program: str) -> Program:
    tokenizer = Tokenizer(program)
    return Program.parse(tokenizer)


p1 = parse('a(1 2).a(1 4).a(5 6).d(X Y Z): ~a(1 Y).')
p1.validate()
p2 = parse('''edge(a, b).
              edge(b, c).
              edge(c, d).
              edge(d, c).
              tc(X, Y) : edge(X, Y).
              tc(X, Y) : tc(X, Z), tc(Z, Y).''')
p2.validate()
