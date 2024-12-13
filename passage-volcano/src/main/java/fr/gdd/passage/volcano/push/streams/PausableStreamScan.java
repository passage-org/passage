package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op0;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PausableStreamScan<ID,VALUE> implements PausableStream<ID, VALUE> {

    final PassageSplitScan<ID,VALUE> wrapped;
    final PassageExecutionContext<ID,VALUE> context;

    public PausableStreamScan(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, Op0 tripleOrQuad) {
        this.wrapped = new PassageSplitScan<>(context, input, tripleOrQuad);
        this.context = context;
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
