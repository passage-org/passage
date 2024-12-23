package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.push.Pause2Continuation;
import fr.gdd.passage.volcano.querypatterns.IsSkippableQuery;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class PausableStreamLimitOffset<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final PausableStream<ID,VALUE> wrapped;
    final BackendBindings<ID,VALUE> input;
    final OpSlice slice;
    final PassagePushExecutor<ID,VALUE> executor;

    final AtomicLong actuallyProduced = new AtomicLong();
    final AtomicLong totalProduced = new AtomicLong();

    public PausableStreamLimitOffset(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpSlice slice) {
        this.context =  context;
        this.executor = context.getContext().get(BackendConstants.EXECUTOR);
        this.slice = slice;
        this.input = input;

        PassagePushExecutor<ID,VALUE> newExecutor = new IsSkippableQuery().visit((Op) slice) ?
                new PassagePushExecutor<>(
                        // must be a clone so limit and offset are bound only in this subquery
                        new PassageExecutionContext<ID,VALUE>(((PassageExecutionContext<?, ?>) context).clone())
                                .setLimit(slice.getLength())
                                .setOffset(slice.getStart())):
                new PassagePushExecutor<>(
                        new PassageExecutionContext<ID,VALUE>(((PassageExecutionContext<?, ?>) context).clone())
                                .setLimit(null)
                                .setOffset(null));
        this.wrapped = newExecutor.visit(slice.getSubOp(), context.bindingsFactory.get());
    }

    public Stream<BackendBindings<ID,VALUE>> stream() {
        Stream<BackendBindings<ID,VALUE>> out = wrapped.stream();

        // if it can be skipped, the offset should be handled in the sub-executor
        // we don't count as it will be done in the subquery
        if (!(new IsSkippableQuery().visit((Op) slice))) {
            // Otherwise, the sub-query is complex.
            // `skip` and `limit` cannot be used because they propagate downstream, so the logic at this level
            // is not executed despite its necessity.
            if (slice.getStart() != Long.MIN_VALUE) { // offset:
                out = out.filter(ignored -> totalProduced.incrementAndGet() > slice.getStart());
            }
            if (slice.getLength() != Long.MIN_VALUE) { // limit:
                out = out.filter(ignored -> actuallyProduced.incrementAndGet() <= slice.getLength());
            }
        }

        // if empty, the result is obviously compatible
        if (!input.isEmpty()) { // otherwise, additional check
            out = out.filter(i -> i.isCompatible(input))
                    // TODO multiple parents instead of copy? (add layers of visibility)
                    .map(i -> new BackendBindings<>(i).setParent(input));
        }

        return out;
    }

    @Override
    public Op pause() {
        Op inside = this.wrapped.pause();
        if (Pause2Continuation.isDone(inside)) { return Pause2Continuation.DONE; }
        if (Pause2Continuation.notExecuted(inside, slice.getSubOp())) {
            // the input comes from the production of somewhere, so we must save it here
            return input.joinWith(slice);
        }

        Op subquery;
        if (new IsSkippableQuery().visit((Op) slice)) {
            subquery = inside;
        } else {
            if (slice.getLength() != Long.MIN_VALUE && actuallyProduced.longValue() >= slice.getLength()) { return Pause2Continuation.DONE; }
            long pausedLimit = slice.getLength() == Long.MIN_VALUE ? Long.MIN_VALUE : slice.getLength() - actuallyProduced.longValue();
            long pausedOffset = slice.getStart() == Long.MIN_VALUE ? Long.MIN_VALUE : slice.getStart() - totalProduced.longValue();
            pausedOffset = pausedOffset > 0 ? pausedOffset : Long.MIN_VALUE; // simplify when `OFFSET 0`
            subquery = new OpSlice(inside, pausedOffset, pausedLimit);
        }

        return input.joinWith(subquery);
    }
}
