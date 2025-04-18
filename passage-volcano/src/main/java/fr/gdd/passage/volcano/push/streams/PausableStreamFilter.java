package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.engines.BackendPushExecutor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpFilter;

import java.util.stream.Stream;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;

public class PausableStreamFilter<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final PausableStream<ID,VALUE> wrapped;
    final BackendPushExecutor<ID,VALUE> executor;
    final OpFilter filter;

    public PausableStreamFilter(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpFilter filter) {
        this.executor = context.executor;
        this.wrapped = (PausableStream<ID, VALUE>) executor.visit(filter.getSubOp(), input);
        this.filter = filter;
        this.context = context;
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return wrapped.stream().filter(i -> filter.getExprs().isSatisfied(i, context));
    }

    @Override
    public Op pause() {
        Op subop = wrapped.pause();
        if (notExecuted(filter.getSubOp(), subop)) return filter;
        if (isDone(subop)) return DONE;
        return OpCloningUtil.clone(filter, subop);
    }
}
