#!/usr/bin/env python3.8
# @generated by pegen from grammar.peg

import ast
import sys
import tokenize

from typing import Any, Optional

from pegen.parser import memoize, memoize_left_rec, logger, Parser

import token
from io import StringIO
from tokenize import generate_tokens
from pegen.tokenizer import Tokenizer
import dast


def parse_number(n):
    try:
        return int(n)
    except ValueError:
        return float(n)


def token_gen(readline):
    """Whitespace is not significant in datalog so we filter out irrelevant tokens
    This makes the grammar simpler."""
    ignored_token_types = {token.NEWLINE, token.INDENT, token.DEDENT}
    return filter(lambda tok: tok.type not in ignored_token_types, generate_tokens(readline))


def parse_file(filename: str) -> dast.Program:
    with open(filename) as file:
        return _parse(file.readline)


def parse_string(string: str) -> dast.Program:
    return _parse(StringIO(string).readline)


def _parse(readline) -> dast.Program:
    tokenizer = Tokenizer(token_gen(readline), verbose=False)
    parser = DatalogParser(tokenizer, verbose=False)
    return parser.start()

# Keywords and soft keywords are listed at the end of the parser definition.
class DatalogParser(Parser):

    @memoize
    def start(self) -> Optional[Any]:
        # start: query | retraction | program
        # nullable=True
        mark = self._mark()
        if (
            (query := self.query())
        ):
            return query;
        self._reset(mark)
        if (
            (retraction := self.retraction())
        ):
            return retraction;
        self._reset(mark)
        if (
            (program := self.program())
        ):
            return program;
        self._reset(mark)
        return None;

    @memoize
    def query(self) -> Optional[dast . Atom]:
        # query: atom '?'
        mark = self._mark()
        if (
            (atom := self.atom())
            and
            (self.expect('?'))
        ):
            return atom;
        self._reset(mark)
        return None;

    @memoize
    def retraction(self) -> Optional[dast . Atom]:
        # retraction: atom '!'
        mark = self._mark()
        if (
            (atom := self.atom())
            and
            (self.expect('!'))
        ):
            return atom;
        self._reset(mark)
        return None;

    @memoize
    def program(self) -> Optional[dast . Program]:
        # program: clause*
        # nullable=True
        mark = self._mark()
        tok = self._tokenizer.peek()
        start_lineno, start_col_offset = tok.start
        if (
            (clauses := self._loop0_1(),)
        ):
            tok = self._tokenizer.get_last_non_whitespace_token()
            end_lineno, end_col_offset = tok.end
            return dast . Program ( clauses , location = dict ( lineno=start_lineno, col_offset=start_col_offset, end_lineno=end_lineno, end_col_offset=end_col_offset ) );
        self._reset(mark)
        return None;

    @memoize
    def clause(self) -> Optional[dast . Clause]:
        # clause: fact | rule
        mark = self._mark()
        if (
            (fact := self.fact())
        ):
            return fact;
        self._reset(mark)
        if (
            (rule := self.rule())
        ):
            return rule;
        self._reset(mark)
        return None;

    @memoize
    def fact(self) -> Optional[dast . Fact]:
        # fact: atom '.'
        mark = self._mark()
        tok = self._tokenizer.peek()
        start_lineno, start_col_offset = tok.start
        if (
            (atom := self.atom())
            and
            (self.expect('.'))
        ):
            tok = self._tokenizer.get_last_non_whitespace_token()
            end_lineno, end_col_offset = tok.end
            return dast . Fact ( atom , location = dict ( lineno=start_lineno, col_offset=start_col_offset, end_lineno=end_lineno, end_col_offset=end_col_offset ) );
        self._reset(mark)
        return None;

    @memoize
    def rule(self) -> Optional[dast . Rule]:
        # rule: atom ':' (','.premise+) '.'
        mark = self._mark()
        tok = self._tokenizer.peek()
        start_lineno, start_col_offset = tok.start
        if (
            (head := self.atom())
            and
            (self.expect(':'))
            and
            (body := self._gather_2())
            and
            (self.expect('.'))
        ):
            tok = self._tokenizer.get_last_non_whitespace_token()
            end_lineno, end_col_offset = tok.end
            return dast . Rule ( head , body , location = dict ( lineno=start_lineno, col_offset=start_col_offset, end_lineno=end_lineno, end_col_offset=end_col_offset ) );
        self._reset(mark)
        return None;

    @memoize
    def atom(self) -> Optional[dast . Atom]:
        # atom: NAME '(' ','.term+ ')'
        mark = self._mark()
        tok = self._tokenizer.peek()
        start_lineno, start_col_offset = tok.start
        if (
            (pred_sym := self.name())
            and
            (self.expect('('))
            and
            (args := self._gather_4())
            and
            (self.expect(')'))
        ):
            tok = self._tokenizer.get_last_non_whitespace_token()
            end_lineno, end_col_offset = tok.end
            return dast . Atom ( pred_sym . string , args , location = dict ( lineno=start_lineno, col_offset=start_col_offset, end_lineno=end_lineno, end_col_offset=end_col_offset ) );
        self._reset(mark)
        return None;

    @memoize
    def negated_atom(self) -> Optional[dast . Atom]:
        # negated_atom: '~' atom
        mark = self._mark()
        if (
            (literal := self.expect('~'))
            and
            (atom := self.atom())
        ):
            return [literal, atom];
        self._reset(mark)
        return None;

    @memoize
    def premise(self) -> Optional[Any]:
        # premise: atom | negated_atom
        mark = self._mark()
        if (
            (atom := self.atom())
        ):
            return atom;
        self._reset(mark)
        if (
            (negated_atom := self.negated_atom())
        ):
            return negated_atom;
        self._reset(mark)
        return None;

    @memoize
    def term(self) -> Optional[dast . Term]:
        # term: string_term | number_term | variable_term
        mark = self._mark()
        if (
            (string_term := self.string_term())
        ):
            return string_term;
        self._reset(mark)
        if (
            (number_term := self.number_term())
        ):
            return number_term;
        self._reset(mark)
        if (
            (variable_term := self.variable_term())
        ):
            return variable_term;
        self._reset(mark)
        return None;

    @memoize
    def string_term(self) -> Optional[dast . StringTerm]:
        # string_term: STRING
        mark = self._mark()
        tok = self._tokenizer.peek()
        start_lineno, start_col_offset = tok.start
        if (
            (string := self.string())
        ):
            tok = self._tokenizer.get_last_non_whitespace_token()
            end_lineno, end_col_offset = tok.end
            return dast . StringTerm ( string . string [1 : - 1] , location = dict ( lineno=start_lineno, col_offset=start_col_offset, end_lineno=end_lineno, end_col_offset=end_col_offset ) );
        self._reset(mark)
        return None;

    @memoize
    def number_term(self) -> Optional[dast . NumberTerm]:
        # number_term: NUMBER
        mark = self._mark()
        tok = self._tokenizer.peek()
        start_lineno, start_col_offset = tok.start
        if (
            (number := self.number())
        ):
            tok = self._tokenizer.get_last_non_whitespace_token()
            end_lineno, end_col_offset = tok.end
            return dast . NumberTerm ( parse_number ( number . string ) , location = dict ( lineno=start_lineno, col_offset=start_col_offset, end_lineno=end_lineno, end_col_offset=end_col_offset ) );
        self._reset(mark)
        return None;

    @memoize
    def variable_term(self) -> Optional[dast . VariableTerm]:
        # variable_term: NAME
        mark = self._mark()
        tok = self._tokenizer.peek()
        start_lineno, start_col_offset = tok.start
        if (
            (variable := self.name())
        ):
            tok = self._tokenizer.get_last_non_whitespace_token()
            end_lineno, end_col_offset = tok.end
            return dast . VariableTerm ( variable . string , location = dict ( lineno=start_lineno, col_offset=start_col_offset, end_lineno=end_lineno, end_col_offset=end_col_offset ) );
        self._reset(mark)
        return None;

    @memoize
    def _loop0_1(self) -> Optional[Any]:
        # _loop0_1: clause
        mark = self._mark()
        children = []
        while (
            (clause := self.clause())
        ):
            children.append(clause)
            mark = self._mark()
        self._reset(mark)
        return children;

    @memoize
    def _loop0_3(self) -> Optional[Any]:
        # _loop0_3: ',' premise
        mark = self._mark()
        children = []
        while (
            (self.expect(','))
            and
            (elem := self.premise())
        ):
            children.append(elem)
            mark = self._mark()
        self._reset(mark)
        return children;

    @memoize
    def _gather_2(self) -> Optional[Any]:
        # _gather_2: premise _loop0_3
        mark = self._mark()
        if (
            (elem := self.premise())
            is not None
            and
            (seq := self._loop0_3())
            is not None
        ):
            return [elem] + seq;
        self._reset(mark)
        return None;

    @memoize
    def _loop0_5(self) -> Optional[Any]:
        # _loop0_5: ',' term
        mark = self._mark()
        children = []
        while (
            (self.expect(','))
            and
            (elem := self.term())
        ):
            children.append(elem)
            mark = self._mark()
        self._reset(mark)
        return children;

    @memoize
    def _gather_4(self) -> Optional[Any]:
        # _gather_4: term _loop0_5
        mark = self._mark()
        if (
            (elem := self.term())
            is not None
            and
            (seq := self._loop0_5())
            is not None
        ):
            return [elem] + seq;
        self._reset(mark)
        return None;

    KEYWORDS = ()
    SOFT_KEYWORDS = ()


if __name__ == '__main__':
    from pegen.parser import simple_parser_main
    simple_parser_main(DatalogParser)
