package fr.gdd.passage.volcano.exceptions;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Exception thrown by the backjump iterators when they
 * fail to produce any result based on a variable.
 */
public class BackjumpException extends RuntimeException {

    public final Set<Var> problematicVariables;

    public BackjumpException(Set<Var> problematicVariables){
        super(null, null, false, false);
        this.problematicVariables = problematicVariables;
    }

    public BackjumpException(Op op, BackendBindings<?,?> bindings) {
        this.problematicVariables = switch (op) {
            case OpTriple t -> {
                Set<Var> vars = OpVars.visibleVars(t);
                vars.retainAll(bindings.variables());
                yield vars;
            }
            default -> new HashSet<>();
        };
    }

    public boolean matches(Set<Var> produced) {
        return produced.stream().anyMatch(problematicVariables::contains);
    }

    @Override
    public String toString() {
        return "BackjumpException on " + Arrays.toString(problematicVariables.toArray());
    }
}