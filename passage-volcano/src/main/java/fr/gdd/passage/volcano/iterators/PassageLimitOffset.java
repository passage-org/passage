package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.factories.IBackendLimitOffsetFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.volcano.PassageConstants;
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

        if (canSkip) { // only OFFSET (optionally LIMIT) for simple sub-query comprising a single TP/QP
            PassageExecutionContext<ID,VALUE> subContext = ((PassageExecutionContext<ID, VALUE>) context).clone();
            subContext.setLimit(slice.getLength());
            subContext.setOffset(slice.getStart());
            subContext.setQuery(slice.getSubOp());
            // TODO remove input I think.
            return new PassageSubOpExecutor<ID,VALUE>(subContext).visit(slice.getSubOp(), input);
        }

        if (slice.getLength() != Long.MIN_VALUE && slice.getStart() == Long.MIN_VALUE) { // only LIMIT
            PassageExecutionContext<ID,VALUE> subContext = ((PassageExecutionContext<ID, VALUE>) context).clone();
            subContext.setLimit(slice.getLength());
            subContext.setOffset(slice.getStart());
            subContext.setQuery(slice.getSubOp());
            BackendSaver<ID,VALUE,?> saver = context.getContext().get(PassageConstants.SAVER);
            PassageLimit<ID,VALUE> it = new PassageLimit<>(subContext, slice);
            saver.register(slice, it);
            return it;
        }

        throw new UnsupportedOperationException("OFFSET on complex queries is not supported.");
    }

}
