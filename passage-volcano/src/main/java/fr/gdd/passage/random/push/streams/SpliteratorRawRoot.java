package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.random.push.PassRawPushExecutor;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import fr.gdd.passage.volcano.push.streams.PausableStream;
import org.apache.jena.sparql.algebra.Op;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * The root of random walks. It's design to loop until all allocated resources are exhausted.
 * For instance, if the engine is designed to timeout after 60s, this will ensure that the
 * query execution lasts 60s, even though sub-iterators must run for lower amount of time.
 */
public class SpliteratorRawRoot<ID,VALUE> implements PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    PausableStream<ID,VALUE> wrapped;
    final PassRawPushExecutor<ID,VALUE> executor;
    final Op root;
    final BackendBindings<ID,VALUE> input;
    long limit;

    public SpliteratorRawRoot(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, Op root) {
        this.context = context;
        this.executor = (PassRawPushExecutor<ID, VALUE>) context.executor;
        this.wrapped = executor.visit(root, input); // check if could be a problem to inject the input in the subquery
        this.root = root;
        this.input = input;
        this.limit = 10_000_000;
    }

    public SpliteratorRawRoot<ID,VALUE> setLimit(long limit) {this.limit = limit; return this;}

    @Override
    public Op pause() {
        return wrapped.pause();
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        while (context.stoppingConditions.stream().noneMatch(p -> p.test(context))) {
            try {
                boolean advanced = wrapped.stream().spliterator().tryAdvance(action);
                if (!advanced) {
                    this.wrapped = executor.visit(root, input); // check if could be a problem to inject the input in the subquery
                } else {
                    context.incrementNbResults();
                }
            } catch (Exception e) {
                limit -= 1;
                // do nothing to try again
            }
            limit -= 1;
        }
        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        if (limit > 2) { // take care of half the walks
            long leftLimit = limit / 2;
            long rightLimit = limit / 2 + limit % 2;
            setLimit(leftLimit);
            new SpliteratorRawRoot<>(context, input, root).setLimit(rightLimit);
        }
        return null;
    }

    @Override
    public long estimateSize() {
        return wrapped.stream().spliterator().estimateSize();
    }

    @Override
    public int characteristics() {
        return wrapped.stream().spliterator().characteristics();
    }
}
