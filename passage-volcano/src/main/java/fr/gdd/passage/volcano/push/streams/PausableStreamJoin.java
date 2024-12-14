package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PausableStreamJoin<ID,VALUE> implements PausableStream<ID, VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final OpJoin join;
    final SpliteratorJoin<ID,VALUE> wrapped;

    public PausableStreamJoin(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpJoin join) {
        this.join = join;
        this.context = context;
        this.wrapped = new SpliteratorJoin<>(context, input, join);
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
