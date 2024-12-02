package fr.gdd.passage.volcano.spliterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.iterators.limitoffset.CanBeSkipped;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class PassagePushLimitOffset<ID,VALUE> extends PausableSpliterator<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>> {

    final PassageExecutionContext<ID,VALUE> context;
    final Stream<BackendBindings<ID,VALUE>> wrapped;
    final OpSlice slice;
    final PassagePushExecutor<ID,VALUE> executor;

    final AtomicLong produced = new AtomicLong();

    public PassagePushLimitOffset(ExecutionContext context, BackendBindings<ID,VALUE> input, OpSlice slice) {
        super((PassageExecutionContext<ID, VALUE>) context, slice);
        this.context =  (PassageExecutionContext<ID, VALUE>) context;
        this.executor = context.getContext().get(BackendConstants.EXECUTOR);
        this.slice = slice;

        if (new CanBeSkipped().visit((Op) slice)) {
            PassagePushExecutor<ID,VALUE> newExecutor = new PassagePushExecutor<>(
                    new PassageExecutionContext<ID,VALUE>(context)
                            .setLimit(slice.getLength())
                            .setOffset(slice.getStart()));
            // skip and offset should be handled in the sub-executor
            this.wrapped = newExecutor.visit(slice.getSubOp(), new BackendBindings<>())
                    .map(i -> i.isCompatible(input) ? input.setParent(i) : null)
                    .filter(Objects::nonNull);
        } else { // but sometimes, operators do not provide efficient skips, so we can stay in this context
            this.wrapped = executor.visit(slice.getSubOp(), new BackendBindings<>())
                    .skip(slice.getStart() == Long.MIN_VALUE ? 0 : slice.getStart())
                    .limit(slice.getLength() == Long.MIN_VALUE ? Long.MAX_VALUE : slice.getLength())
                    .peek(i -> produced.getAndIncrement())
                    .map(i -> i.isCompatible(input) ? input.setParent(i) : null)
                    .filter(Objects::nonNull);
        }
    }

    public Stream<BackendBindings<ID,VALUE>> stream() {
        return this.wrapped;
    }


    /* ****************************** SPLITERATOR **************************** */

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        return this.wrapped.spliterator().tryAdvance(action);
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        return this.wrapped.spliterator().trySplit();
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return this.wrapped.spliterator().characteristics();
    }
}
