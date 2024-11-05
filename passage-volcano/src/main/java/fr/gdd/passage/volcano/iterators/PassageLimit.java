package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

/**
 * No input. The sub query must be evaluated all alone.
 */
public class PassageLimit<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final ExecutionContext context;
    final OpSlice slice;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;
    long produced = 0L;

    public PassageLimit (ExecutionContext context, OpSlice slice) {
        this.context = context;
        this.slice = slice;
        BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
        this.wrapped = executor.visit(slice.getSubOp(), Iter.of(new BackendBindings<>()));
    }

    @Override
    public boolean hasNext() {
        return Objects.nonNull(wrapped) && wrapped.hasNext() && produced < slice.getLength();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        produced += 1;
        return wrapped.next();
    }

    public Op pause (Op inside) {
        if (!hasNext()) return null;
        return new OpSlice(inside, Long.MIN_VALUE, slice.getLength() - produced);
    }
}
