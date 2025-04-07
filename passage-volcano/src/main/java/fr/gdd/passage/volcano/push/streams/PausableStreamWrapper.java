package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.random.push.streams.SpliteratorBackJump;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.jena.sparql.algebra.Op;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple converter from `PausableSpliterator` to `PausableStream`.
 */
public class PausableStreamWrapper<ID,VALUE,OP extends Op> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final OP op;
    final PausableSpliterator<ID,VALUE> wrapped;

    public PausableStreamWrapper(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OP op,
                                 TriFunction<PassageExecutionContext<ID,VALUE>, BackendBindings<ID,VALUE>, OP,
                                         PausableSpliterator<ID,VALUE>> supplier) {
        this.op = op;
        this.context = context;
        this.wrapped = supplier.apply(context, input, op);
    }

    public PausableStreamWrapper(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OP op,
                                 TriFunction<PassageExecutionContext<ID,VALUE>, BackendBindings<ID,VALUE>, OP,
                                         PausableSpliterator<ID,VALUE>> supplier, boolean enableBackJump) {
        this.op = op;
        this.context = context;
        this.wrapped = enableBackJump ?
                new SpliteratorBackJump<>(context, input, op, supplier.apply(context, input, op)):
                supplier.apply(context, input, op);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return StreamSupport.stream(wrapped, context.maxParallelism > 1);
    }

    @Override
    public Op pause() {
        return wrapped.pause();
    }

}
