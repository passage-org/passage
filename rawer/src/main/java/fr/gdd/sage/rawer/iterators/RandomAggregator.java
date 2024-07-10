package fr.gdd.sage.rawer.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.BackendSaver;
import fr.gdd.sage.interfaces.BackendAccumulator;
import fr.gdd.sage.rawer.RawerConstants;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.rawer.accumulators.CountDistinctCRAWD;
import fr.gdd.sage.rawer.accumulators.WanderJoinCount;
import fr.gdd.sage.rawer.accumulators.CountDistinctFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * TODO maybe think of a common class for sager and rawer, since they should only
 * TODO differ in the stopping condition.
 */
public class RandomAggregator<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    // TODO /!\ This is ugly. There should be a better way to devise
    // TODO a budget defined by a configuration, or adaptive, or etc.
    // TODO should check that one at least is set.
    public static long SUBQUERY_LIMIT = Long.MAX_VALUE;
    public static long SUBQUERY_TIMEOUT = Long.MAX_VALUE;

    final RawerOpExecutor<ID,VALUE> executor;
    final OpGroup op;
    final Iterator<BackendBindings<ID,VALUE>> input;

    BackendBindings<ID,VALUE> inputBinding;
    Pair<Var, BackendAccumulator<ID,VALUE>> var2accumulator = null;

    public RandomAggregator(RawerOpExecutor<ID, VALUE> executor, OpGroup op, Iterator<BackendBindings<ID,VALUE>> input){
        this.executor = executor;
        this.op = op;
        this.input = input;

        for (ExprAggregator agg : op.getAggregators() ) {
            BackendAccumulator<ID,VALUE> sagerX = switch (agg.getAggregator()) {
                case AggCount ac -> new WanderJoinCount<>(executor.getExecutionContext(), op.getSubOp());
                case AggCountVarDistinct acvd -> {
                    CountDistinctFactory<ID,VALUE> factory = executor.getExecutionContext().getContext().get(RawerConstants.COUNT_DISTINCT_FACTORY);
                    if (Objects.isNull(factory)) { // default is CRAWD
                        factory = CountDistinctCRAWD::new;
                    };
                    yield factory.create(acvd.getExprList(), executor.getExecutionContext(), op);
                }
                default -> throw new UnsupportedOperationException("The aggregator is not supported yet.");
            };
            Var v = agg.getVar();
            var2accumulator = new ImmutablePair<>(v, sagerX);
        }

        BackendSaver<ID,VALUE,?> saver = executor.getExecutionContext().getContext().get(RawerConstants.SAVER);
        saver.register(op, this);
    }

    public BackendAccumulator<ID,VALUE> getAccumulator() {return var2accumulator.getRight();}

    @Override
    public boolean hasNext() {
        return this.input.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        inputBinding = input.next();
        long limit = executor.getExecutionContext().getContext().getLong(RawerConstants.LIMIT, Long.MAX_VALUE);
        long deadline = executor.getExecutionContext().getContext().getLong(RawerConstants.DEADLINE, Long.MAX_VALUE);
        while (System.currentTimeMillis() < deadline &&
                executor.getExecutionContext().getContext().getLong(RawerConstants.SCANS, 0L) < limit) {
            // Because we don't go through executor.execute, we don't wrap our iterator with a
            // validity checker, therefore, it might not have a next, hence bindings being null.
            Iterator<BackendBindings<ID,VALUE>> subquery = ReturningArgsOpVisitorRouter.visit(executor, op.getSubOp(), Iter.of(inputBinding));
            BackendBindings<ID,VALUE> bindings = null;
            if (subquery.hasNext()) {
                bindings = subquery.next();
            }
            // BackendBindings<ID,VALUE> keyBinding = getKeyBinding(op.getGroupVars().getVars(), bindings);

            var2accumulator.getRight().accumulate(bindings, executor.getExecutionContext()); // bindings can be null
        }

        return createBinding(var2accumulator);
    }


    /* ************************************************************************* */

    public static <ID,VALUE> BackendBindings<ID,VALUE> getKeyBinding(Set<Var> vars, BackendBindings<ID,VALUE> binding) {
        BackendBindings<ID,VALUE> keyBinding = new BackendBindings<>();
        vars.forEach(v -> keyBinding.put(v, binding.get(v)));
        return keyBinding;
    }

    private BackendBindings<ID,VALUE> createBinding(Pair<Var, BackendAccumulator<ID,VALUE>> var2acc) {
        BackendBindings<ID,VALUE> newBinding = new BackendBindings<>();
        newBinding.put(var2acc.getLeft(), new BackendBindings.IdValueBackend<ID,VALUE>()
                .setBackend(executor.getBackend())
                .setValue(var2acc.getRight().getValue()));
        return newBinding;
    }

}
