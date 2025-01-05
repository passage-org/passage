package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpDistinct;

import java.util.stream.Stream;

@Deprecated // TODO
public class PausableStreamDistinct<ID,VALUE> implements PausableStream<ID, VALUE> {

    final OpDistinct distinct;
    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final PausableStream<ID, VALUE> wrapped;
    BackendBindings<ID,VALUE> lastProduced;

    public PausableStreamDistinct(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpDistinct distinct) {
        this.distinct = distinct;
        this.context = context;
        this.input = input;
        // at this point the wrapped should be a triple/quad pattern with associated offset
        // TODO use: this.context.backend.searchDistinct()
        this.wrapped = null; // TODO use spliterators of distinct scan
        if (context.maxParallelism > 1) {
            // TODO make sure that scans provide unique values even when split
            //      When the last values of the preceding scan where already produced by the scan with higher offset.
            throw new IllegalArgumentException("maxParallelism > 1");
        }
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return wrapped.stream().peek(binding -> lastProduced = binding);
    }

    @Override
    public Op pause() {
        // {DISTINCT PROJECT {TP OFFSET}} FILTER last value

        return null;
    }
}
