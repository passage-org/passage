package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.pull.iterators.PassageCountAccumulator;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.E_Add;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static fr.gdd.passage.volcano.push.Pause2Continuation.notExecuted;

public class PausableStreamCount<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final OpGroup count;
    final BackendBindings<ID,VALUE> keys;
    final Map<Var, BackendAccumulator<ID,VALUE>> var2accumulator;
    final PausableStream<ID,VALUE> wrapped;
    final Map<Var, Set<Var>> jenaId2queryId = new HashMap<>();

    public PausableStreamCount(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpGroup count) {
        this.input = input;
        this.context = context;
        this.count = count;
        this.var2accumulator = context.maxParallelism > 1 ? new ConcurrentHashMap<>() : new HashMap<>();

        keys = getKeyBinding(count.getGroupVars().getVars(), input);
        if (keys.variables().stream().anyMatch(v -> Objects.isNull(keys.getBinding(v)))) {
            // TODO if variables that are in keys are not in the input,
            //      then they should be enumerated as a DISTINCT iterator.
            //      Continuation queries on DISTINCT might be feasible only
            //      under some conditions.
            throw new UnsupportedOperationException("Unbounded GROUP BY keys in COUNT are not supported yet.");
        }

        for (ExprAggregator agg : count.getAggregators() ) {
            Set<Var> vars; // TODO ugly, should be improved
            if (Objects.nonNull(agg.getAggregator().getExprList())) {
                vars = agg.getAggregator().getExprList().getVarsMentioned();
                jenaId2queryId.put(agg.getVar(), vars);
            } else {
                vars = agg.getVarsMentioned();
                jenaId2queryId.put(agg.getVar(), vars);
            }
            BackendAccumulator<ID,VALUE> passageCount = switch (agg.getAggregator()) {
                case AggCount ignored -> new PassageCountAccumulator<>(context, count, vars);
                case AggCountVar ignored -> new PassageCountAccumulator<>(context, count, vars);
                default -> throw new UnsupportedOperationException("The aggregator is not supported.");
            };
            Var varIdOfJena = agg.getVar();
            var2accumulator.put(varIdOfJena, passageCount);
        }

        // automatically join with the input, but only on keys
        wrapped = ((PassagePushExecutor<ID,VALUE>) context.executor).visit(count.getSubOp(), keys);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        wrapped.stream()
                .forEach(i -> {
                    // for (Var var2count : count.getAggregators().stream().map(ExprAggregator::getVar).toList()) {
                    for (ExprAggregator agg : count.getAggregators()) {
                        var2accumulator.get(agg.getVar()).accumulate(i, context);
                    }
                });
        BackendBindings<ID,VALUE> counts = context.bindingsFactory.get();
        for (Map.Entry<Var, BackendAccumulator<ID,VALUE>> entry : var2accumulator.entrySet()) {
            counts.put(entry.getKey(), new BackendBindings.IdValueBackend<ID,VALUE>()
                    .setValue(entry.getValue().getValue()));
        }

        if (!keys.isEmpty()) { // if no key match, it means that it should not produce a result, instead of "0"
            for (BackendAccumulator<ID,VALUE> acc : var2accumulator.values()) {
                if (!acc.gotIncremented()) { return Stream.of(); }
            }
        }

        return Stream.of(counts.setParent(input));
    }

    @Override
    public Op pause() {
        Op inside = wrapped.pause();
        if (notExecuted(this.count.getSubOp(), inside)) return input.joinWith(count);

        Op cloned = input.joinWith(OpCloningUtil.clone(count, inside));

        for (Map.Entry<Var, BackendAccumulator<ID,VALUE>> entry : var2accumulator.entrySet()) {
            PassageCountAccumulator<ID,VALUE> acc = (PassageCountAccumulator<ID,VALUE>) entry.getValue(); // TODO not cast
            cloned = OpExtend.create(cloned, entry.getKey(),
                    new E_Add(new ExprVar(entry.getKey()),
                            NodeValue.makeInteger(acc.getCount())));
        };

        return cloned;
    }

    /* ************************************* UTILS ************************************* */

    private BackendBindings<ID,VALUE> getKeyBinding(List<Var> vars, BackendBindings<ID,VALUE> binding) {
        BackendBindings<ID,VALUE> keyBinding = context.bindingsFactory.get();
        vars.forEach(v -> keyBinding.put(v, binding.getBinding(v)));
        return keyBinding;
    }

}
