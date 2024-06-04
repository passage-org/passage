package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

/**
 * A distinct iterator that only check if the last one is the same, because it assumes
 * an order by where bindings that are identical are produced are contiguous.
 * Only works in this case, which is enough for us for now.
 */
public class SagerDistinct<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final ExecutionContext context;
    final OpDistinct op;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;
    final Save2SPARQL<ID,VALUE> saver;

    BackendBindings<ID,VALUE> lastBinding = new BackendBindings<>();
    BackendBindings<ID,VALUE> newBinding;

    public SagerDistinct(OpDistinct op, ExecutionContext context, Iterator<BackendBindings<ID,VALUE>> wrapped) {
        this.context = context;
        this.op = op;
        this.wrapped = wrapped;
        this.saver = context.getContext().get(SagerConstants.SAVER);
        this.saver.register(op, this);
    }

    @Override
    public boolean hasNext() {
        if (Objects.nonNull(newBinding)) return true;

        while (wrapped.hasNext()) {
            BackendBindings<ID,VALUE> produced = wrapped.next();
            if (!produced.equals(lastBinding)) {
                newBinding = produced;
                return true;
            }
        }

        return false;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        lastBinding = newBinding;
        newBinding = null; // consumed
        return lastBinding;
    }
}
