import networkx as nx


class Term:
    pass


class StringTerm(Term):
    def __init__(self, value, **location):
        self.value = value
        self.location = location

    def __repr__(self):
        return f"StringTerm(value={self.value})"

    def __eq__(self, other) -> bool:
        return isinstance(other, StringTerm) and other.value == self.value


class NumberTerm(Term):
    def __init__(self, value, **location):
        self.value = value
        self.location = location

    def __repr__(self):
        return f"NumberTerm(value={self.value})"

    def __eq__(self, other) -> bool:
        return isinstance(other, NumberTerm) and other.value == self.value


class VariableTerm(Term):
    def __init__(self, name, value=None, **location):
        self.name = name
        self.value = value
        self.location = location

    def __repr__(self):
        return f"VariableTerm(name={self.name}, value={self.value})"

    def __eq__(self, other) -> bool:
        return (self.value is None or
                (isinstance(other, NumberTerm) and other.value == self.value) or
                (isinstance(other, StringTerm) and other.value == self.value))


class Atom:
    def __init__(self, pred_sym: str, args: list[Term], **location):
        self.pred_sym = pred_sym
        self.args = args
        self.location = location

    @property
    def arity(self) -> int:
        return len(self.args)

    @property
    def variables(self) -> list[VariableTerm]:
        return [a for a in self.args if isinstance(a, VariableTerm)]

    def __repr__(self):
        return f"Atom(pred_sym={self.pred_sym}, args=[{','.join(map(str, self.args))}])"

    def __eq__(self, other) -> bool:
        return (isinstance(other, Atom)
                and self.pred_sym == other.pred_sym
                and self.arity == other.arity
                and self.args == other.args)


class Clause:
    pass


class Fact(Clause):
    def __init__(self, head: Atom, **location):
        self.head = head
        self.location = location

    @property
    def pred_sym(self) -> str:
        return self.head.pred_sym

    @property
    def arity(self) -> int:
        return self.head.arity

    def __repr__(self):
        return f"Fact(head={self.head})"


class Rule(Clause):
    def __init__(self, head: Atom, premises: list[Atom], **location):
        self.head = head
        self.premises = premises
        self.location = location

    @property
    def pred_sym(self) -> str:
        return self.head.pred_sym

    @property
    def arity(self) -> int:
        return self.head.arity

    def __repr__(self):
        return f"Rule(head={self.head}, premises=[{','.join(map(str, self.premises))}])"


class Program:
    def __init__(self, clauses: list[Clause] = [], **location):
        self.clauses = clauses
        self.location = location

        self.dependency_graph = nx.DiGraph()
        for rule in self.rules:
            src_key = rule.pred_sym, rule.arity
            for premise in rule.premises:
                dst_key = premise.pred_sym, premise.arity
                self.dependency_graph.add_edge(src_key, dst_key)

    @property
    def facts(self) -> list[Fact]:
        return [c for c in self.clauses if isinstance(c, Fact)]

    @property
    def rules(self) -> list[Rule]:
        return [c for c in self.clauses if isinstance(c, Rule)]

    def __repr__(self):
        return f"Program(clauses=[{','.join(map(str, self.clauses))}])"
