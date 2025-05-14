import parser
from edb import EDB
from validator import NonRecursiveValidator
from pandas_evaluator import PandasEvaluator


def repl(edb: EDB, filename: str, validator=NonRecursiveValidator, evaluator=PandasEvaluator):
    while True:
        try:
            program = parser.parse_file(filename)
            errors = list(validator(program).validate())
            if len(errors) != 0:
                print(errors)
            ev = evaluator(program)
            edb.insert_facts(program.facts)
            query = parser.parse_string(input("> "))
            res = ev.query(query)
            edb.
        except KeyboardInterrupt:
            return


if __name__ == '__main__':
    import sys

    res = run(sys.argv[1])
    print(res)
