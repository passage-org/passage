package fr.gdd.passage.volcano.spliterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class PassagePushLimitOffset<ID,VALUE> {

    final ExecutionContext context;
    final Stream<BackendBindings<ID,VALUE>> wrapped;
    final OpSlice slice;
    final PassagePushExecutor<ID,VALUE> executor;

    final AtomicLong produced = new AtomicLong();

    public PassagePushLimitOffset(ExecutionContext context, BackendBindings<ID,VALUE> input, OpSlice slice) {
        this.context =  context;
        this.executor = context.getContext().get(BackendConstants.EXECUTOR);

        this.wrapped = executor.visit(slice.getSubOp(), new BackendBindings<>())
                .skip(slice.getStart() == Long.MIN_VALUE ? 0 : slice.getStart())
                .limit(slice.getLength() == Long.MIN_VALUE ? Long.MAX_VALUE : slice.getLength())
                .peek(i -> produced.getAndIncrement())
                .map(i -> i.isCompatible(input) ? input.setParent(i) : null)
                .filter(Objects::nonNull);;
        this.slice = slice;
    }

    public Stream<BackendBindings<ID,VALUE>> stream() {
        return this.wrapped;
    }

}
