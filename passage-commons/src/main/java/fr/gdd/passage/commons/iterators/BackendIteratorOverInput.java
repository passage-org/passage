package fr.gdd.passage.commons.iterators;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendIteratorFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

/**
 * Create some kind of iterator of <OP> for each input.
 */
public class BackendIteratorOverInput<ID,VALUE,OP> implements Iterator<BackendBindings<ID,VALUE>> {

    final ExecutionContext context;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final OP op;
    final IBackendIteratorFactory<ID,VALUE,OP> factory;

    Iterator<BackendBindings<ID,VALUE>> wrapped;
    boolean consumed = true;

    public BackendIteratorOverInput(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, OP op, IBackendIteratorFactory<ID,VALUE,OP> factory) {
        this.context = context;
        this.op = op;
        this.input = input;
        this.factory = factory;
    }

    @Override
    public boolean hasNext() {
        if (!consumed) {return true;} // hasNext has been called but next is not.
        if ((Objects.isNull(wrapped) || !wrapped.hasNext()) && !input.hasNext()) return false;
        if (Objects.nonNull(wrapped) && !wrapped.hasNext()) { wrapped = null; } // reset

        // enumerate the input
        while (Objects.isNull(wrapped) && input.hasNext()) {
            BackendBindings<ID, VALUE> bindings = input.next();

            try {
                wrapped = factory.get(context, bindings, op);
                if (!wrapped.hasNext()) { wrapped = null; } // nothing for this input
            } catch (NotFoundException | IllegalArgumentException e) { // important for Scan, among other things
                wrapped = null; // nothing for this input
            }
        }

        consumed = Objects.isNull(wrapped); // as if consumed since nothing

        return !consumed;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        consumed = true;
        return wrapped.next();
    }
}
