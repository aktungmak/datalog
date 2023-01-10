import token
from collections import deque
from io import StringIO
from tokenize import generate_tokens, TokenInfo
from typing import Optional


class Tokenizer:
    ignored_token_types = [token.NEWLINE]

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

        print(f"consume {expected} {self.last}")
        return self.last

    def try_consume(self, expected: str, expected_type: Optional[int] = None) -> bool:
        tok = self.consume()
        print(f"try_consume {expected} {tok}")
        if tok.string == expected or tok.type == expected_type:
            return True
        else:
            self._queue.append(tok)
            return False


class Program:
    @classmethod
    def parse(cls, tokenizer: Tokenizer):
        clauses = [clause for clause in Clause.parse_all(tokenizer)]
        return Program(clauses)

    def __init__(self, clauses: list):
        self.clauses = clauses


class Clause:
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer):
        atom = Atom.parse(tokenizer)

        if tokenizer.try_consume("."):
            return Fact(atom)
        elif tokenizer.try_consume(":"):
            body = [p for p in Premise.parse_all(tokenizer)]
            return Rule(atom, body)
        else:
            tok = tokenizer.consume()
            raise ParseError(f"invalid terminator", tok)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer):
        while True:
            yield Clause.parse_one(tokenizer)
            if tokenizer.try_consume("", expected_type=token.ENDMARKER):
                break


class Premise:
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer):
        if tokenizer.try_consume("~"):
            atom = Atom.parse(tokenizer)
            return NegatedAtom(atom)
        else:
            atom = Atom.parse(tokenizer)
            return PositiveAtom(atom)

    @classmethod
    def parse_all(cls, tokenizer: Tokenizer):
        while True:
            yield Premise.parse_one(tokenizer)
            if tokenizer.try_consume(","):
                continue
            else:
                tokenizer.consume(".")
                break


class Atom:
    @classmethod
    def parse(cls, tokenizer: Tokenizer):
        pred_sym_token = tokenizer.consume()
        tokenizer.consume("(")
        args = [t for t in Term.parse_all(tokenizer)]
        return Atom(pred_sym_token, args)

    def __init__(self, pred_sym_token: TokenInfo, args: list):
        self._pred_sym_token = pred_sym_token
        self.pred_sym = pred_sym_token.string
        self.args = args


class Fact(Atom):
    def __init__(self, atom: Atom):
        print(f"Fact: {atom}")
        self.atom = atom


class PositiveAtom(Atom):
    def __init__(self, atom: Atom):
        print(f"PositiveAtom: {atom}")
        self.atom = atom


class NegativeAtom(Atom):
    def __init__(self, atom: Atom):
        print(f"NegativeAtom: {atom}")
        self.atom = atom


class Rule:
    def __init__(self, head: Atom, body: list):
        print(f"Rule: {head} {body}")
        self.head = head
        self.body = body


class Term:
    @classmethod
    def parse_one(cls, tokenizer: Tokenizer):
        tok = tokenizer.consume()
        if tok.type == token.NAME and tok.string[0].isupper():
            return VariableTerm(tok)
        elif (
            tok.type == token.NAME and tok.string[0].islower()
        ) or tok.type == token.STRING:
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


class StringTerm(Term):
    def __init__(self, tok: TokenInfo):
        self.tok = tok
        self.value = tok.string


class NumberTerm(Term):
    def __init__(self, tok: TokenInfo):
        self.tok = tok
        self.value = float(tok.string)


class ParseError(Exception):
    def __init__(self, message, tok):
        location = f" on line {tok.start[0]}, pos {tok.start[1]}"
        caret = " " * tok.start[1] + "^"
        self.message = "\n".join([message + location, tok.line, caret])
        self.tok = tok
        super().__init__(self.message)
