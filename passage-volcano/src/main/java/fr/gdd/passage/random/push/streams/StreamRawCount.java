package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.engines.BackendPushExecutor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.streams.PausableStream;
import fr.gdd.raw.accumulators.AccumulatorFactory;
import fr.gdd.raw.accumulators.CountDistinctCRAWD;
import fr.gdd.raw.accumulators.CountWanderJoin;
import fr.gdd.raw.executor.RawConstants;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Approximate count using WanderJoin as basis, built in a streaming fashion.
 */
public class StreamRawCount<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final BackendPushExecutor<ID,VALUE> executor;
    final BackendBindings<ID,VALUE> input;
    final OpGroup op;
    final PausableStream<ID,VALUE> wrapped;

    final Map<Var, BackendAccumulator<ID,VALUE>> var2accumulator = new HashMap<>();

    public StreamRawCount(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpGroup count) {
        if (!input.isEmpty()) {
            throw new UnsupportedOperationException("Nested aggregate are not supported just yet");
        }
        this.context = context;
        this.executor = context.executor;
        this.input = input;
        this.op = count;
        this.wrapped = (PausableStream<ID, VALUE>) executor.visit(op.getSubOp(), input);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return Stream.empty();
    }

    @Override
    public Op pause() {
        return null;
    }

    /* ******************************** UTIL *********************************** */

    /*public static <ID,VALUE> Map<Var, BackendAccumulator<ID,VALUE>> createVar2Accumulator(PassageExecutionContext<ID,VALUE> context, List<ExprAggregator> aggregators) {
        Map<Var, BackendAccumulator<ID,VALUE>> var2accumulator = new HashMap<>();
        for (ExprAggregator agg : aggregators) {
            BackendAccumulator<ID,VALUE> sagerX = switch (agg.getAggregator()) {
                case AggCount ac -> new CountWanderJoin<>(executor.getExecutionContext(), op.getSubOp());
                case AggCountVarDistinct acvd -> {
                    AccumulatorFactory<ID,VALUE> factory = executor.getExecutionContext().getContext().get(RawConstants.COUNT_DISTINCT_FACTORY);
                    if (Objects.isNull(factory)) { // default is CRAWD
                        factory = CountDistinctCRAWD::new;
                    };
                    yield factory.create(acvd.getExprList(), executor.getExecutionContext(), op);
                }
                default -> throw new UnsupportedOperationException("The aggregator is not supported yet.");
            };
            Var v = agg.getVar();
            return new ImmutablePair<>(v, sagerX);
        }
        return null;
    }*/
}
