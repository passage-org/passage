package fr.gdd.passage.volcano.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.factories.IBackendLimitOffsetFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageSubOpExecutor;
import fr.gdd.passage.volcano.resume.IsSkippable;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

/**
 * Does not actually process any LIMIT OFFSET. It checks if it's a valid
 * subquery (i.e., one that can be paused/resumed) then it executes it.
 * Preemption mostly comes from this: the ability to start over from an OFFSET efficiently.
 * When we find a pattern like SELECT * WHERE {?s ?p ?o} OFFSET X, the engine know that
 * it must skip X elements of the iterator. But the pattern must be accurate: a single
 * triple pattern.
 */
public class PassageLimitOffset<ID,VALUE> implements IBackendLimitOffsetFactory<ID,VALUE> {

    @Override
    public Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, OpSlice slice) {
        Backend<ID,VALUE,Long> backend = context.getContext().get(BackendConstants.BACKEND);
        Boolean isSkippable = new IsSkippable().visit(slice);

        if (isSkippable) {
            // TODO create new context
            PassageExecutionContext<ID,VALUE> newContext = new PassageExecutionContext<>(backend);
            PassageSubOpExecutor<ID,VALUE,Long> subExec = new PassageSubOpExecutor<>(newContext);
            return ReturningArgsOpVisitorRouter.visit(subExec, slice, input);
        }
        // TODO otherwise it's a normal slice (TODO) handle it or never
        throw new UnsupportedOperationException("TODO Default LIMIT OFFSET not implemented yet.");
    }

}
