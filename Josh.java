import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.util.*;

interface Premise {
    static Premise parse(DatalogTokenizer dt) throws DatalogParseException {
        if (dt.peek().equals("~")) {
            return NegativeAtom.parse(dt);
        } else {
            return PositiveAtom.parse(dt);
        }
    }
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

class Program {
    final Set<Clause> clauses;

    Program() {
        this.clauses = new HashSet<>();
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
        for (Clause clause : this.clauses) {
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
        for (Clause clause : this.clauses) {
            sb.append(clause.toString()).append('\n');
        }
        return sb.toString();
    }

    void addClauses(List<Clause> clauses) {
        this.clauses.addAll(clauses);
    }

    FactCollection evaluate(FactCollection edb) {
        //TODO
        return null;
    }
}

class Clause {
    final PositiveAtom head;
    final List<Premise> body;

    Clause(PositiveAtom head, List<Premise> body) {
        this.head = head;
        this.body = body;
    }

    static List<Clause> parse(DatalogTokenizer dt) throws DatalogParseException {
        ArrayList<Clause> clauses = new ArrayList<>();

        PositiveAtom head = PositiveAtom.parse(dt);
        ArrayList<Premise> body = new ArrayList<>();

        // handle facts
        if (dt.peek().equals(".")) {
            dt.consumeExpected(".");
            clauses.add(new Clause(head, body));
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
                    clauses.add(new Clause(head, body));
                    body = new ArrayList<>();
                    break;
                case ".":
                    clauses.add(new Clause(head, body));
                    break loop;
                default:
                    throw new DatalogParseException("unexpected terminator");
            }
        }
        return clauses;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.head.toString());
        if (this.body.size() > 0) {
            sb.append(": \n");
        }
        for (Premise premise : this.body) {
            sb.append(premise.toString()).append(",\n");
        }
        return sb.append('.').toString();
    }

    List<DatalogValidationError> validate() {
        if (this.body.size() == 0) {
            return validateAsFact();
        } else {
            return validateAsRule();
        }
    }

    List<DatalogValidationError> validateAsFact() {
        ArrayList<DatalogValidationError> errors = new ArrayList<>();
        for (Term arg : this.head.args) {
            if (arg instanceof VariableTerm) {
                errors.add(new DatalogValidationError(this, "Variable term as argument to a fact"));
            }
        }
        return errors;
    }

    List<DatalogValidationError> validateAsRule() {
        ArrayList<DatalogValidationError> errors = new ArrayList<>();
        // TODO implement rule validation
        // range restriction: all variables in head appear in body
        // safety cond: every var must appear in one positive atom
        return errors;
    }
}

class PositiveAtom implements Premise, Serializable {
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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.predicateSymbol).append("(");
        for (Term arg : this.args) {
            sb.append(arg.toString()).append(", ");
        }
        return sb.append(')').toString();
    }

    Row toRow() {
        return RowFactory.create(this.args.toArray());
    }

    StructType getStructType() {
        StructType st = new StructType();
        DataType dt;
        int i = 1;
        for (Term arg : this.args) {
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

    NegativeAtom(PositiveAtom pa) {
        this.atom = pa;
    }

    static NegativeAtom parse(DatalogTokenizer dt) throws DatalogParseException {
        dt.consumeExpected("~");
        PositiveAtom atom = PositiveAtom.parse(dt);
        return new NegativeAtom(atom);
    }

    @Override
    public String toString() {
        return "~" + this.atom.toString();
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
        switch (this.st.ttype) {
            case StreamTokenizer.TT_EOF:
                throw new DatalogParseException("Unexpected EOF");
            case StreamTokenizer.TT_EOL:
                throw new AssertionError("invalid tokenizer state1");
            case StreamTokenizer.TT_NUMBER:
                throw new AssertionError("invalid tokenizer state2");
            case StreamTokenizer.TT_WORD:
                return this.st.sval;
            default:
                return Character.toString((char) token);
        }
    }

    String next() throws DatalogParseException {
        try {
            return tokenToString(this.st.nextToken());
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
}

class FactCollection {
    HashMap<String, List> facts;

    FactCollection() {
        this.facts = new HashMap<>();
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
        this.facts = edb1;
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
        for (Clause c : p.clauses) {
            c.head.getStructType();
            System.out.println(c.head.toRow());
        }
        System.out.println(p);
    }
}
