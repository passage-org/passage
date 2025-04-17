package fr.gdd.passage.commons.factories;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.engines.BackendPullExecutor;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

/**
 * Does nothing by itself, only call the iterator in the right order.
 */
public class BackendNestedLoopJoinFactory<ID,VALUE> implements IBackendJoinsFactory<ID,VALUE> {

    @Override
    public Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, OpJoin join) {
        BackendPullExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
        input = ReturningArgsOpVisitorRouter.visit(executor, join.getLeft(), input);
        return ReturningArgsOpVisitorRouter.visit(executor, join.getRight(), input);
    }
}
