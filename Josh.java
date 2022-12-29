import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

interface Premise {
    static Premise parse(DatalogTokenizer dt) throws DatalogParseException {
        if (dt.peek().equals("~")) {
            return NegativeAtom.parse(dt);
        } else {
            return PositiveAtom.parse(dt);
        }
    }

    List<VariableTerm> variables();
}

interface Term {
    static Term parse(String token) throws DatalogParseException {
        if (token.matches("[A-Z][A-Za-z0-9_]*")) {
            return new VariableTerm(token);
        } else if (token.matches("[a-z][A-Za-z0-9_]*")) {
            return new ConstantTerm(token);
        } else if (token.matches("[0-9]*")) {
            return new NumberTerm(token);
        } else {
            throw new DatalogParseException("cannot parse Term " + token);
        }
    }
}

interface Clause {
    static List<Clause> parse(DatalogTokenizer dt) throws DatalogParseException {
        ArrayList<Clause> clauses = new ArrayList<>();

        PositiveAtom head = PositiveAtom.parse(dt);
        ArrayList<Premise> body = new ArrayList<>();

        // handle facts
        if (dt.peek().equals(".")) {
            dt.consumeExpected(".");
            clauses.add(new Fact(head));
            return clauses;
        }

        // handle rules
        dt.consumeExpected(":");
        loop:
        while (dt.hasNext()) {
            body.add(Premise.parse(dt));
            switch (dt.next()) {
                case ",":
                    break;
                case ";":
                    clauses.add(new Rule(head, body));
                    body = new ArrayList<>();
                    break;
                case ".":
                    clauses.add(new Rule(head, body));
                    break loop;
                default:
                    throw new DatalogParseException("unexpected terminator");
            }
        }
        return clauses;
    }

    List<DatalogValidationError> validate();
}

class Program {
    final Set<Clause> clauses;

    Program() {
        clauses = new HashSet<>();
    }

    static Program parse(DatalogTokenizer dt) throws DatalogParseException {
        Program p = new Program();
        while (dt.hasNext()) {
            p.addClauses(Clause.parse(dt));
        }
        return p;
    }

    List<DatalogValidationError> validate() {
        ArrayList<DatalogValidationError> errors = new ArrayList<>();
        for (Clause clause : clauses) {
            errors.addAll(clause.validate());
        }
        return errors;
    }

    List<Program> stratify() {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Clause clause : clauses) {
            sb.append(clause.toString()).append('\n');
        }
        return sb.toString();
    }

    void addClauses(List<Clause> newClauses) {
        clauses.addAll(newClauses);
    }

    FactCollection evaluate(FactCollection edb) {
        //TODO
        return null;
    }

    List<Fact> facts() {
        return clauses.stream()
                .filter(Fact.class::isInstance)
                .map(Fact.class::cast)
                .collect(Collectors.toList());
    }
}

class Fact implements Clause {
    final PositiveAtom atom;

    Fact(PositiveAtom atom) {
        this.atom = atom;
    }

    @Override
    public String toString() {
        return atom.toString();
    }

    Row toRow() {
        return RowFactory.create(atom.args.toArray());
    }

    StructType getStructType() {
        StructType st = new StructType();
        DataType dt = null;
        int i = 1;
        for (Term arg : atom.args) {
            if (arg instanceof ConstantTerm) {
                dt = DataTypes.StringType;
            } else if (arg instanceof NumberTerm) {
                dt = DataTypes.IntegerType;
            }
            st.add(String.valueOf(i), dt, false);
        }
        return st;
    }

    public List<DatalogValidationError> validate() {
        ArrayList<DatalogValidationError> errors = new ArrayList<>();
        for (Term arg : atom.args) {
            if (arg instanceof VariableTerm) {
                errors.add(new DatalogValidationError(this, "Variable term as argument to a fact"));
            }
        }
        return errors;
    }
}


class Rule implements Clause {
    final PositiveAtom head;
    final List<Premise> body;

    Rule(PositiveAtom head, List<Premise> body) {
        this.head = head;
        this.body = body;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(head.toString());
        if (body.size() > 0) {
            sb.append(": \n");
        }
        for (Premise premise : body) {
            sb.append(premise.toString()).append(",\n");
        }
        return sb.append('.').toString();
    }

    public List<DatalogValidationError> validate() {
        ArrayList<DatalogValidationError> errors = new ArrayList<>();
        // range restriction: all variables in head appear in body
        Set<VariableTerm> headVars = new HashSet<>(head.variables());
        Set<VariableTerm> bodyVars = new HashSet<>(bodyVariables());
        headVars.removeAll(bodyVars);
        if (headVars.size() > 0) {
            errors.add(new DatalogValidationError(headVars, "unrestricted head vars: "));
        }
        // safety cond: every var must appear in one positive atom
        Set<VariableTerm> posVars = new HashSet<>();
        Set<VariableTerm> negVars = new HashSet<>();
        for (Premise p : body) {
            if (p instanceof PositiveAtom) {
                posVars.addAll(p.variables());
            } else if (p instanceof NegativeAtom) {
                negVars.addAll(p.variables());
            }
        }
        negVars.removeAll(posVars);
        if (negVars.size() > 0) {
            errors.add(new DatalogValidationError(negVars, "neg vars not in pos premise: "));
        }
        return errors;
    }

    Set<VariableTerm> bodyVariables() {
        return body.stream()
                .flatMap(p -> p.variables().stream())
                .collect(Collectors.toSet());
    }
}

class PositiveAtom implements Premise {
    final int arity;
    final String predicateSymbol;
    final List<Term> args;

    PositiveAtom(String predicateSymbol, List<Term> args) {
        this.predicateSymbol = predicateSymbol;
        this.args = args;
        this.arity = args.size();
    }

    static PositiveAtom parse(DatalogTokenizer dt) throws DatalogParseException {
        String predicateSymbol = dt.next();
        ArrayList<Term> args = new ArrayList<>();

        dt.consumeExpected("(");
        while (true) {
            String token = dt.next();
            if (token.equals(")")) {
                break;
            }
            args.add(Term.parse(token));
            dt.consumeOptional(",");
        }
        return new PositiveAtom(predicateSymbol, args);
    }

    @Override
    public List<VariableTerm> variables() {
        return args.stream()
                .filter(VariableTerm.class::isInstance)
                .map(VariableTerm.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(predicateSymbol).append("(");
        for (Term arg : args) {
            sb.append(arg.toString()).append(", ");
        }
        return sb.append(')').toString();
    }

    Row toRow() {
        return RowFactory.create(args.toArray());
    }

    StructType getStructType() {
        StructType st = new StructType();
        DataType dt = null;
        int i = 1;
        for (Term arg : args) {
            if (arg instanceof ConstantTerm) {
                dt = DataTypes.StringType;
            } else if (arg instanceof NumberTerm) {
                dt = DataTypes.IntegerType;
            }
            st.add(String.valueOf(i), dt, false);
        }
        return st;
    }
}

class NegativeAtom implements Premise {
    final PositiveAtom atom;

    NegativeAtom(PositiveAtom atom) {
        this.atom = atom;
    }

    static NegativeAtom parse(DatalogTokenizer dt) throws DatalogParseException {
        dt.consumeExpected("~");
        PositiveAtom atom = PositiveAtom.parse(dt);
        return new NegativeAtom(atom);
    }

    @Override
    public List<VariableTerm> variables() {
        return atom.variables();
    }

    @Override
    public String toString() {
        return "~" + atom.toString();
    }


}

class VariableTerm implements Term {
    String name;

    VariableTerm(String name) {
        this.name = name;
    }

    // Term applySubstitution() {}

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VariableTerm that = (VariableTerm) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return 0;
    }
}

class ConstantTerm implements Term {
    String name;

    ConstantTerm(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}


class NumberTerm implements Term {
    int value;

    NumberTerm(String token) {
        this.value = Integer.parseInt(token);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}

class DatalogTokenizer {
    final StreamTokenizer st;

    DatalogTokenizer(Reader r) {
        st = new StreamTokenizer(r);
        st.ordinaryChars('0', '9');
        st.ordinaryChar('.');
        st.wordChars('0', '9');
        st.wordChars('_', '_');
    }

    void consumeExpected(String expected) throws DatalogParseException {
        if (!next().equals(expected)) {
            throw new DatalogParseException("expected " + expected);
        }
    }

    private String tokenToString(int token) throws DatalogParseException {
        switch (st.ttype) {
            case StreamTokenizer.TT_EOF:
                throw new DatalogParseException("Unexpected EOF");
            case StreamTokenizer.TT_EOL:
                throw new AssertionError("invalid tokenizer state1");
            case StreamTokenizer.TT_NUMBER:
                throw new AssertionError("invalid tokenizer state2");
            case StreamTokenizer.TT_WORD:
                return st.sval;
            default:
                return Character.toString((char) token);
        }
    }

    String next() throws DatalogParseException {
        try {
            return tokenToString(st.nextToken());
        } catch (IOException e) {
            throw new DatalogParseException(e);
        }
    }

    String peek() throws DatalogParseException {
        try {
            String token = tokenToString(st.nextToken());
            st.pushBack();
            return token;
        } catch (IOException e) {
            throw new DatalogParseException(e);
        }
    }

    boolean hasNext() throws DatalogParseException {
        try {
            st.nextToken();
            st.pushBack();
            return st.ttype != StreamTokenizer.TT_EOF;
        } catch (IOException e) {
            throw new DatalogParseException(e);
        }
    }

    void consumeOptional(String expected) {
        try {
            String token = tokenToString(st.nextToken());
            if (!token.equals(expected)) {
                st.pushBack();
            }
        } catch (DatalogParseException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}

class DatalogParseException extends Exception {
    DatalogParseException(String message) {
        super(message);
    }

    DatalogParseException(Throwable cause) {
        super(cause);
    }
}

class DatalogValidationError {
    final Object astElement;
    final String description;

    DatalogValidationError(Object astElement, String description) {
        this.astElement = astElement;
        this.description = description;
    }

    @Override
    public String toString() {
        return "DatalogValidationError{" +
                "astElement=" + astElement +
                ", description='" + description + '\'' +
                '}';
    }
}

class FactCollection {
    HashMap<String, Dataset> facts;

    FactCollection() {
        facts = new HashMap<>();
    }

}

class BottomUpEngine {
    FactCollection facts;

    void evaluate(Program program) {
        program.validate();
        List<Program> strata = program.stratify();
        FactCollection edb1 = new FactCollection();
        FactCollection edb2 = new FactCollection();

        for (Program stratum : strata) {
            edb2 = stratum.evaluate(edb1);
        }
        facts = edb1;
    }

    Set<PositiveAtom> query(PositiveAtom atom) {
        return new HashSet<>();
    }

}

public class Josh {
    public static void main(String[] args) throws DatalogParseException {
        Reader r = new StringReader("i(3, 4). aaa(A, B): b(b, A, 2), c(c, B). q(a, b). m(F): ~t(F, a); w(s, m).");
        DatalogTokenizer dt = new DatalogTokenizer(r);
        Program p = Program.parse(dt);
        for (Fact f : p.facts()) {
            f.getStructType();
            System.out.println(f.toRow());
        }
        System.out.println(p);
        System.out.println(p.validate());
    }
}
