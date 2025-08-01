package fr.gdd.passage.volcano.pull.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * In charge of counting.
 */
public class PassageCountAccumulator<ID,VALUE> implements BackendAccumulator<ID,VALUE> {

    final ExecutionContext context;
    final OpGroup opCount;
    final Set<Var> vars;

    LongAdder value = new LongAdder();

    public PassageCountAccumulator(ExecutionContext context, OpGroup opCount) {
        this.context = context;
        this.opCount = opCount;
        this.vars = opCount.getAggregators().stream().filter(ea -> Objects.nonNull(ea.getAggregator().getExprList()))
                .map(ea -> ea.getAggregator().getExprList().getVarsMentioned())
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public PassageCountAccumulator(ExecutionContext context, OpGroup opCount, Set<Var> vars) {
        this.vars = vars;
        this.context = context;
        this.opCount = opCount;
    }

    @Override
    public void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv) {
        if (vars.stream().anyMatch(v-> !binding.contains(v))) {
            return; // TODO check that the evaluation of the expression against the binding does not throw an error.
        }
        value.increment();
    }

    @Override
    public VALUE getValue() {
        Backend<ID,VALUE> backend = context.getContext().get(BackendConstants.BACKEND);
        return backend.getValue(String.format("\"%s\"^^<http://www.w3.org/2001/XMLSchema#integer>", value.intValue()));
    }

    public long getCount() {
        return value.longValue();
    }

    @Override
    public boolean gotIncremented() {
        return value.longValue() > 0;
    }
}
