package fr.gdd.passage.volcano.accumulators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

// TODO From SagerAccumulator to BackendAccumulator (EZ but still, todo)
public class PassageAccCount<ID,VALUE> implements PassageAccumulator<ID,VALUE> {

    final ExecutionContext context;
    final Op op;

    Integer value = 0;

    public PassageAccCount(ExecutionContext context, Op subOp) {
        this.context = context;
        this.op = subOp;
    }

    @Override
    public void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv) {
        value += 1;
    }

    @Override
    public VALUE getValue() {
        Backend<ID,VALUE,?> backend = context.getContext().get(PassageConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^xsd:integer", value));
    }
}
