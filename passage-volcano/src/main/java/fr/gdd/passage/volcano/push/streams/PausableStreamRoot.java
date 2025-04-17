package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.sparql.algebra.Op;

import java.util.stream.Stream;

/**
 * Only exist to count the number of produced results. The stopping condition remains
 * checked at scan level. In case of parallel execution, this could lead to slightly more
 * results being produced.
 * *
 * Note: this is also different from LIMIT clauses as it should be defined as a resource
 * constraint by the server (e.g. as done by dbpedia) possibly downsized by the user, on demand.
 */
public class PausableStreamRoot<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final PausableStream<ID,VALUE> wrapped;

    public PausableStreamRoot(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, Op root) {
        this.context = context;
        this.wrapped = ((PassagePushExecutor<ID, VALUE>) context.executor).visit(root, input);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return wrapped.stream().peek(b -> context.incrementNbResults());
    }

    @Override
    public Op pause() {
        return wrapped.pause();
    }
}
