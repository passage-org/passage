package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.volcano.CanBeSkipped;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.push.Pause2Continuation;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public class PassagePushLimitOffset<ID,VALUE> extends PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final Stream<BackendBindings<ID,VALUE>> wrapped;
    final BackendBindings<ID,VALUE> input;
    final OpSlice slice;
    final PassagePushExecutor<ID,VALUE> executor;

    final LongAdder actuallyProduced = new LongAdder();
    final LongAdder totalProduced = new LongAdder();

    public PassagePushLimitOffset(ExecutionContext context, BackendBindings<ID,VALUE> input, OpSlice slice) {
        super((PassageExecutionContext<ID, VALUE>) context, slice);
        this.context =  (PassageExecutionContext<ID, VALUE>) context;
        this.executor = context.getContext().get(BackendConstants.EXECUTOR);
        this.slice = slice;
        this.input = input;

        if (new CanBeSkipped().visit((Op) slice)) {
            PassagePushExecutor<ID,VALUE> newExecutor = new PassagePushExecutor<>(
                    // must be a clone so limit and offset are bound only in this subquery
                    new PassageExecutionContext<ID,VALUE>(((PassageExecutionContext<?, ?>) context).clone())
                            .setLimit(slice.getLength())
                            .setOffset(slice.getStart()));
            // skip and offset should be handled in the sub-executor
            this.wrapped = newExecutor.visit(slice.getSubOp(), new BackendBindings<>())
                    // TODO multiple parents instead of copy? (add layers of visibility)
                    .map(i -> i.isCompatible(input) ? new BackendBindings<>(i).setParent(input) : null)
                    // we don't count as it will be done in the subquery
                    .filter(Objects::nonNull);
        } else { // but sometimes, operators do not provide efficient skips, so we can stay in this context
            this.wrapped = executor.visit(slice.getSubOp(), new BackendBindings<>())
                    .peek(i -> totalProduced.increment())
                    // `skip` and `limit` cannot be used because they propagate downstream, so the logic at this level
                    // is not executed despite its necessity.
                    // offset:
                    .filter(ignored -> slice.getStart() == Long.MIN_VALUE || totalProduced.longValue() > slice.getStart())
                    // limit:
                    .filter(ignored -> slice.getLength() == Long.MIN_VALUE || actuallyProduced.longValue() < slice.getLength())
                    // TODO multiple parents instead of copy? (add layers of visibility)
                    .map(i -> i.isCompatible(input) ? new BackendBindings<>(i).setParent(input) : null)
                    .filter(Objects::nonNull)
                    .peek(i -> actuallyProduced.increment()); // peek after so not compatible mappings are deleted
        }
    }

    public Stream<BackendBindings<ID,VALUE>> stream() {
        return this.wrapped;
    }

    @Override
    public Op pause() {
        Op inside = new Pause2Continuation<>(context.op2its).visit(slice.getSubOp());
        if (Pause2Continuation.isDone(inside)) { return Pause2Continuation.DONE; }
        if (Pause2Continuation.notExecuted(inside, slice.getSubOp())) {
            // the input comes from the production of somewhere, so we must save it here
            return input.joinWith(slice);
        }

        Op subquery;
        if (new CanBeSkipped().visit((Op) slice)) {
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
