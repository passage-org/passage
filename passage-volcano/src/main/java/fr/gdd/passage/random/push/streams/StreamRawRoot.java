package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.random.push.PassRawPushExecutor;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.streams.PausableStream;
import org.apache.jena.sparql.algebra.Op;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * The root of random walks. It's design to loop until all allocated resources are exhausted.
 * For instance, if the engine is designed to timeout after 60s, this will ensure that the
 * query execution lasts 60s, even though sub-iterators must run for lower amount of time.
 */
public class StreamRawRoot<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final PassRawPushExecutor<ID,VALUE> executor;
    final Op root;
    final BackendBindings<ID,VALUE> input;
    final Supplier<PausableStream<ID,VALUE>> streamSupplier;

    public StreamRawRoot(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, Op root) {
        this.context = context;
        this.executor = (PassRawPushExecutor<ID, VALUE>) context.executor;
        // this.wrapped = executor.visit(root, input); // check if could be a problem to inject the input in the subquery
        this.root = root;
        this.input = input;
        this.streamSupplier = () -> executor.visit(root, input);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return Stream.generate(streamSupplier)
                .flatMap(PausableStream::stream)
                .takeWhile(i ->
                        context.stoppingConditions.stream().noneMatch(
                                p -> p.test(context)))
                .peek(i -> context.incrementNbResults());
    }

    @Override
    public Op pause() {
        // TODO register wrapped in flatMap
        /// return wrapped.pause();
        return null;
    }

}
