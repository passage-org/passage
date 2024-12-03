package fr.gdd.passage.volcano.spliterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.iterators.limitoffset.CanBeSkipped;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public class PassagePushLimitOffset<ID,VALUE> extends PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final Stream<BackendBindings<ID,VALUE>> wrapped;
    final OpSlice slice;
    final PassagePushExecutor<ID,VALUE> executor;

    final LongAdder produced = new LongAdder();

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
                    // TODO multiple parents instead of copy? (add layers of visibility)
                    .map(i -> i.isCompatible(input) ? new BackendBindings<>(i).setParent(input) : null)
                    // we don't count as it will be done in the subquery
                    .filter(Objects::nonNull);
        } else { // but sometimes, operators do not provide efficient skips, so we can stay in this context
            this.wrapped = executor.visit(slice.getSubOp(), new BackendBindings<>())
                    .skip(slice.getStart() == Long.MIN_VALUE ? 0 : slice.getStart())
                    .limit(slice.getLength() == Long.MIN_VALUE ? Long.MAX_VALUE : slice.getLength())
                    .peek(i -> produced.increment())
                    // TODO multiple parents instead of copy? (add layers of visibility)
                    .map(i -> i.isCompatible(input) ? new BackendBindings<>(i).setParent(input) : null)
                    .filter(Objects::nonNull);
        }
    }

    public Stream<BackendBindings<ID,VALUE>> stream() {
        return this.wrapped;
    }

}
