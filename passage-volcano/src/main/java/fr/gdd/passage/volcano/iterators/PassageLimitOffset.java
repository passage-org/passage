package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.factories.IBackendLimitOffsetFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageSubOpExecutor;
import fr.gdd.passage.volcano.resume.CanBeSkipped;
import org.apache.jena.sparql.algebra.Op;
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
        Boolean canSkip = new CanBeSkipped().visit((Op) slice);

        if (canSkip) {
            PassageExecutionContext<ID,VALUE> subContext = ((PassageExecutionContext<ID, VALUE>) context).clone();
            subContext.setLimit(slice.getLength());
            subContext.setOffset(slice.getStart());
            subContext.setQuery(slice.getSubOp());
            return new PassageSubOpExecutor<ID,VALUE>(subContext).visit(slice.getSubOp(), input);
        }
        // TODO otherwise it's a normal slice (TODO) handle it or never
        //  We will handle it by +1 on offset when produced result is produced
        //  As a regular query, however, we don't run it as a subquery with limit offset in the execution context
        throw new UnsupportedOperationException("TODO Default LIMIT OFFSET not implemented yet.");
    }

}
