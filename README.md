# Python datalog

This repo contains a datalog interpreter written in Python. It is suitable for
validating and defining complex configuration rules in a user friendly way,
as well as defining and evaluating complex buisiness logic without layers and
layers of if-else statements.

It can also provide traces of why particular rules succeeded or failed.

## Structure

There are several layers that transform code and data into the final output.

### Layer 1: Parser

`parser.py` is generated from `grammar.peg`. It takes a datalog program as a
file or string and produces an AST, as defined in `dast.py`.

### Layer 2: AST

The AST of the language is defined in `dast.py`. These dataclasses do not do
anything more than represent the code and provide an easy way to walk the AST.

### Layer 3: Validator

The code may produce an AST successfully (i.e. syntactically correct) but there
may be semantic issues with the definitions it contains. This will vary based
on the variant of datalog we are expecting to evaluate, e.g. some things are
allowed in stratified datalog with negation that would not be acceptable in 
non-recursive datalog.

The classes in `validator.py` take an AST as input and produce a (possibly
empty) iterator of errors with the program.

### Layer 4: Evaluator

An evaluator takes as input an AST that has presumably been validated by the
appropriate validator already along with an optional EDB of additional facts,
represented as a subclass of `EDB`.

Once instantiated, the evaluator provides a `query()` method that takes an `Atom`
as its argument. It will then return a `Relation` of results that could be 
derived from the program and EDB.

### Layer 5: EDB

To store the facts themselves, implementations of the abstract class `EDB` use
Pandas DataFrames, SQLite tables or other options to provide the underlying
select, project, join and union (SPJU) operations needed to compute the result.

The main goal of this class is to abstract away the details of the underlying
engine so the other layers are not aware of how the SPJU operations are being
performed.