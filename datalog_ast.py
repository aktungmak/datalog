class Term:
    pass


class StringTerm(Term):
    def __init__(self, value, **location):
        self.value = value
        self.location = location

    def __repr__(self):
        return f"StringTerm(value={self.value})"


class NumberTerm(Term):
    def __init__(self, value, **location):
        self.value = value
        self.location = location

    def __repr__(self):
        return f"NumberTerm(value={self.value})"


class VariableTerm(Term):
    def __init__(self, name, value=None, **location):
        self.name = name
        self.value = value
        self.location = location

    def __repr__(self):
        return f"VariableTerm(name={self.name}, value={self.value})"


class Atom:
    def __init__(self, pred_sym: str, args: list[Term], **location):
        self.pred_sym = pred_sym
        self.args = args
        self.location = location

    def __repr__(self):
        return f"Atom(pred_sym={self.pred_sym}, args=[{','.join(map(str, self.args))}])"


class Clause:
    pass


class Fact(Clause):
    def __init__(self, head: Atom, **location):
        self.head = head
        self.location = location

    def __repr__(self):
        return f"Fact(head={self.head})"


class Rule(Clause):
    def __init__(self, head: Atom, premises: list[Atom], **location):
        self.head = head
        self.premises = premises
        self.location = location

    def __repr__(self):
        return f"Rule(head={self.head}, premises=[{','.join(map(str, self.premises))}])"


class Program:
    def __init__(self, clauses: list[Clause] = [], **location):
        self.clauses = clauses
        self.location = location

    def __repr__(self):
        return f"Program(clauses=[{','.join(map(str, self.clauses))}])"
