package fr.gdd.sage.sager.accumulators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

// TODO From SagerAccumulator to BackendAccumulator (EZ but still, todo)
public class SagerAccCount<ID,VALUE> implements SagerAccumulator<ID,VALUE>{

    final ExecutionContext context;
    final Op op;

    Integer value = 0;

    public SagerAccCount(ExecutionContext context, Op subOp) {
        this.context = context;
        this.op = subOp;
    }

    @Override
    public void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv) {
        value += 1;
    }

    @Override
    public VALUE getValue() {
        Backend<ID,VALUE,?> backend = context.getContext().get(SagerConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^xsd:integer", value));
    }
}
