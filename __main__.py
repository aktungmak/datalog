import parser
from dast import Fact, Rule, Program, Atom
from edb import PandasEDB
from pandas_evaluator import PandasEvaluator
from validator import NonRecursiveValidator


class Repl:
    def __init__(self, program_filename: str, validator=NonRecursiveValidator, evaluator=PandasEvaluator):
        program = parser.parse_file(program_filename)
        errors = list(validator(program).validate())
        if len(errors) != 0:
            print(errors)
            exit(1)
        self.evaluator = evaluator(program)

    def loop(self):
        while True:
            try:
                cmd = parser.parse_string(input("> "))
                print(cmd)
                if isinstance(cmd, Program):
                    for clause in cmd.clauses:
                        if isinstance(clause, Fact):
                            self.evaluator.edb.insert_facts([clause])
                        if isinstance(clause, Rule):
                            self.evaluator.prog.rules.append(clause)
                elif isinstance(cmd, Atom):
                    print(self.evaluator.query(cmd))
                else:
                    print(f"unknown command type {type(clause)}")
            except KeyboardInterrupt:
                return


if __name__ == '__main__':
    Repl("sample_programs/good.dl").loop()
