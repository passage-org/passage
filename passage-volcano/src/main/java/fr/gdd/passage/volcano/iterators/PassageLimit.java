package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

/**
 * No input. The sub query must be evaluated all alone.
 */
public class PassageLimit<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final PassageExecutionContext<ID,VALUE> context;
    final OpSlice slice;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;
    long produced = 0L;
    long offset = 0L;
    boolean consumed = true;

    public PassageLimit (ExecutionContext context, OpSlice slice) {
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.slice = slice;
        PassageOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
        this.wrapped = executor.visit(slice.getSubOp(), Iter.of(new BackendBindings<>()));
    }

    @Override
    public boolean hasNext() {
        if (!consumed) { return true; } // found but not consumed yet.
        if (slice.getLength()!=Long.MIN_VALUE && produced >= slice.getLength()) { return false; } // above LIMIT

        while (!context.paused.isPaused() &&
                slice.getStart() != Long.MIN_VALUE && offset < slice.getStart() && // skip to OFFSET
                wrapped.hasNext()) {
            var ignored = wrapped.next(); // thrown away
            ++offset; // but cursor increments
        }

        consumed = !wrapped.hasNext();
        return !consumed;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        consumed = true;
        produced += 1;
        return wrapped.next();
    }

    public Op pause (Op inside) {
        if (slice.getLength()!=Long.MIN_VALUE && produced >= slice.getLength()) { return null; }
        long pausedOffset = offset >= slice.getStart() ? Long.MIN_VALUE : slice.getStart() - offset;
        return new OpSlice(inside, pausedOffset, slice.getLength() - produced);
    }
}
