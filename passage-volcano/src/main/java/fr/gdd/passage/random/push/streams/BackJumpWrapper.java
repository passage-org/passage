package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.exceptions.BackjumpException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

@Deprecated
public class BackJumpWrapper<ID,VALUE> extends SpliteratorRawScan<ID,VALUE> {

    public BackJumpWrapper(PassageExecutionContext<ID, VALUE> context, BackendBindings<ID, VALUE> input, OpTriple op) {
        super(context, input, op);

        if (estimateCardinality() == 0) {
            throw new BackjumpException(op, input);
        }
    }

    public BackJumpWrapper(PassageExecutionContext<ID, VALUE> context, BackendBindings<ID, VALUE> input, OpQuad op) {
        super(context, input, op);

        if (estimateCardinality() == 0) {
            throw new BackjumpException(op, input);
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        try {
            return super.tryAdvance(action);
        } catch (BackjumpException bje) {
            Set<Var> producedVars = getProducedVars(op, input); // TODO lazy
            if (producedVars.stream().noneMatch(bje.problematicVariables::contains)) {
                throw bje;
            }
        }
        return false;
    }


    public static <ID,VALUE> Set<Var> getProducedVars(Op op, BackendBindings<ID,VALUE> input) {
        return switch (op) {
            case OpTriple t -> {
                Set<Var> vars = OpVars.visibleVars(t);
                vars.removeAll(input.variables());
                yield vars;
            }
            default -> new HashSet<>();
        };
    }
}
