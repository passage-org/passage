package fr.gdd.sage.sager.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.accumulators.SagerAccCount;
import fr.gdd.sage.sager.accumulators.SagerAccumulator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.aggregate.AggCount;

import java.util.*;

public class SagerAgg<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final SagerOpExecutor<ID,VALUE> executor;
    final OpGroup op;
    final Iterator<BackendBindings<ID,VALUE>> input;

    Pair<Var, SagerAccumulator<ID,VALUE>> var2accumulator = null;

    public SagerAgg (SagerOpExecutor<ID, VALUE> executor, OpGroup op, Iterator<BackendBindings<ID,VALUE>> input){
        this.executor = executor;
        this.op = op;
        this.input = input;

        for (ExprAggregator agg : op.getAggregators() ) {
            SagerAccumulator<ID,VALUE> sagerX = switch (agg.getAggregator()) {
                case AggCount ac -> new SagerAccCount<>(executor.getExecutionContext(), op.getSubOp());
                default -> throw new UnsupportedOperationException("The aggregator is not supported yet.");
            };
            Var v = agg.getVar();
            var2accumulator = new ImmutablePair<>(v, sagerX);
        }
    }

    @Override
    public boolean hasNext() {
        return this.input.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        Iterator<BackendBindings<ID,VALUE>> subop = ReturningArgsOpVisitorRouter.visit(executor, op.getSubOp(), Iter.of(input.next()));

        while (subop.hasNext()) {
            BackendBindings<ID,VALUE> bindings = subop.next();
            // BackendBindings<ID,VALUE> keyBinding = getKeyBinding(op.getGroupVars().getVars(), bindings);

            var2accumulator.getRight().accumulate(bindings, executor.getExecutionContext());
        }

        return createBinding(var2accumulator);
    }

    private static <ID,VALUE> BackendBindings<ID,VALUE> getKeyBinding(List<Var> vars, BackendBindings<ID,VALUE> binding) {
        BackendBindings<ID,VALUE> keyBinding = new BackendBindings<>();
        vars.forEach(v -> keyBinding.put(v, binding.get(v)));
        return keyBinding;
    }

    private BackendBindings<ID,VALUE> createBinding(Pair<Var, SagerAccumulator<ID,VALUE>> var2acc) {
        BackendBindings<ID,VALUE> newBinding = new BackendBindings<>();
        newBinding.put(var2acc.getLeft(), new BackendBindings.IdValueBackend<ID,VALUE>()
                .setBackend(executor.getBackend())
                .setValue(var2acc.getRight().getValue()));
        return newBinding;
    }

}
