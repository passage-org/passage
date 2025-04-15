package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.random.push.PassRawPushExecutor;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.streams.PausableStream;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpGroup;

import java.util.stream.Stream;

/**
 * Approximate count using WanderJoin as basis, built in a streaming fashion.
 */
public class StreamRawCount<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final PassRawPushExecutor<ID,VALUE> executor;
    final BackendBindings<ID,VALUE> input;
    final OpGroup op;
    final PausableStream<ID,VALUE> wrapped;

    public StreamRawCount(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpGroup count) {
        this.context = context;
        this.executor = (PassRawPushExecutor<ID,VALUE>) context.executor;
        this.input = input;
        this.op = count;
        this.wrapped = executor.visit(op.getSubOp(), input);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return Stream.empty();
    }

    @Override
    public Op pause() {
        return null;
    }
}
