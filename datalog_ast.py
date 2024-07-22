import token
from collections import deque, defaultdict
from io import StringIO
from tokenize import generate_tokens, TokenInfo
from typing import Optional, Self

import networkx as nx


class ParseError(Exception):
    def __init__(self, message, tok):
        location = f" on line {tok.start[0]}, pos {tok.start[1]}"
        caret = " " * tok.start[1] + "^"
        self.message = "\n".join([message + location, tok.line, caret])
        self.tok = tok
        super().__init__(self.message)


class ValidationError(Exception):
    def __init__(self, message, term=None):
        self.message = message
        self.term = term
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

    def try_consume(self, expected: str = None, expected_type: Optional[int] = None) -> bool:
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
            if tokenizer.try_consume('<'):
                return AggTerm.parse(tok, tokenizer)
            else:
                return StringTerm(tok)
        elif tok.type == token.STRING:
            return StringTerm(tok)
        elif tok.type == token.NUMBER:
            return NumberTerm(tok)
        else:
            raise ParseError("invalid term", tok)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer) -> list[Self]:
        terms = []
        while True:
            if tokenizer.try_consume(")"):
                break
            terms.append(Term.parse_one(tokenizer))
            tokenizer.try_consume(",")
        return terms

    def __eq__(self, other) -> bool:
        if isinstance(self, VariableTerm) or isinstance(other, VariableTerm):
            return True
        elif isinstance(self, StringTerm) and isinstance(other, StringTerm):
            return self.value == other.value
        elif isinstance(self, NumberTerm) and isinstance(other, NumberTerm):
            return self.value == other.value
        else:
            return False


class AggTerm(Term):
    @classmethod
    def parse(cls, func: TokenInfo, tokenizer: Tokenizer) -> Self:
        args = []
        while True:
            tok = tokenizer.consume()
            if tok.string == '>':
                return AggTerm(func, args)
            elif tok.type == token.NAME and tok.string[0].isupper():
                args.append(tok)
            else:
                raise ParseError("invalid aggregation parameter", tok)
            tokenizer.try_consume(",")

    def __init__(self, func: TokenInfo, args: list[TokenInfo] = []):
        self.func_tok = func
        self.arg_toks = args
        self.func = func.string
        self.args = [VariableTerm(arg) for arg in args]

    def __repr__(self) -> str:
        return f"AggTerm(func={self.func}, args={self.args})"


class VariableTerm(Term):
    def __init__(self, tok: TokenInfo = None, value=None):
        self.tok = tok
        self.name = tok.string
        self.value = value

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
    def parse_string(cls, atom: str) -> Self:
        tokenizer = Tokenizer(atom)
        return Atom.parse(tokenizer)

    @classmethod
    def parse(cls, tokenizer: Tokenizer, negated: bool = False) -> Self:
        pred_sym_token = tokenizer.consume()
        tokenizer.consume("(")
        args = Term.parse_all(tokenizer)
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

    def __eq__(self, other) -> bool:
        return isinstance(other, Atom) \
               and self.pred_sym == other.pred_sym \
               and self.arity == other.arity \
               and self.args == other.args


class Premise(Atom):
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer) -> Self:
        negated = tokenizer.try_consume("~")
        return Atom.parse(tokenizer, negated=negated)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer) -> list[Self]:
        premises = []
        while True:
            premises.append(Premise.parse_one(tokenizer))
            tokenizer.try_consume(",")
            if tokenizer.try_consume("."):
                break
        return premises


class Clause:
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer) -> Self:
        head = Atom.parse(tokenizer)

        if tokenizer.try_consume("."):
            return Fact(head)
        elif tokenizer.try_consume(":"):
            body = Premise.parse_all(tokenizer)
            return Rule(head, body)
        else:
            tok = tokenizer.consume()
            raise ParseError(f"invalid terminator", tok)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer) -> list[Self]:
        clauses = []
        while True:
            clauses.append(Clause.parse_one(tokenizer))
            if tokenizer.try_consume(expected_type=token.ENDMARKER):
                break
        return clauses


class Fact(Clause):
    def __init__(self, atom: Atom):
        self._atom = atom
        self.pred_sym = atom.pred_sym
        self.arity = atom.arity
        self.args = atom.args

    def validate(self) -> list[ValidationError]:
        return [
            ValidationError(f"non-ground arg '{var.tok.string}' in fact", var)
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
        return sum([self.validate_range_restriction(), self.validate_negation_safety(), self.validate_aggregation()],
                   [])

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

    def validate_aggregation(self):
        errs = []
        aggs = [arg for arg in self.head.args if isinstance(arg, AggTerm)]
        body_vars = {v for p in self.body for v in p.vars}
        for agg in aggs:
            undefined_vars = set(agg.args) - body_vars
            errs += [ValidationError(f"undefined var '{v.name}' used in aggregation", v) for v in undefined_vars]
        return errs

    def __repr__(self) -> str:
        return f"Rule(head={self.head}, body={self.body})"


class Program:

    @classmethod
    def parse_string(cls, program: str) -> Self:
        tokenizer = Tokenizer(program)
        return Program.parse(tokenizer)

    @classmethod
    def parse(cls, tokenizer: Tokenizer) -> Self:
        clauses = Clause.parse_all(tokenizer)
        return Program(clauses)

    def __init__(self, clauses: list[Clause]):
        self.clauses = clauses
        self.facts = defaultdict(list)
        self.rules = defaultdict(list)
        for clause in clauses:
            if isinstance(clause, Fact):
                self.facts[(clause.pred_sym, clause.arity)].append(clause)
            elif isinstance(clause, Rule):
                self.rules[(clause.pred_sym, clause.arity)].append(clause)
            else:
                raise ValueError(f"not a clause: {clause}")

        self.dependency_graph = nx.DiGraph()
        for rule_key, rules in self.rules.items():
            for rule in rules:
                for premise in rule.body:
                    self.dependency_graph.add_edge(rule_key, (premise.pred_sym, premise.arity))

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
    assert (len(p1.validate_nonrecursive()) > 0)
    p2 = Program.parse_string(s2)
    assert (p2.validate_nonrecursive() == [])
    print(p2.rules)
    print(p2.dependency_graph)
