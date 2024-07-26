import parser
from validator import NonRecursiveValidator
from pandas_evaluator import PandasEvaluator

def run(filename, validator=NonRecursiveValidator, evaluator=PandasEvaluator):
    program = parser.parse_file(filename)
    errors = list(validator(program).validate())
    if len(errors) != 0:
        print(errors)
    return evaluator(program).run()

if __name__ == '__main__':
    import sys
    res = run(sys.argv[1])
    print(res)