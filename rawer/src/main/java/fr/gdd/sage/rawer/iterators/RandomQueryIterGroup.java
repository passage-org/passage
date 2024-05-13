package fr.gdd.sage.rawer.iterators;

import fr.gdd.sage.generics.BackendBindings;

import java.util.Iterator;

/**
 * Similar to {@link org.apache.jena.sparql.engine.iterator.QueryIterGroup} but
 * with different possible backends.
 * @param <ID> The identifiers of the backend.
 * @param <VALUE> The values of the backend.
 */
public class RandomQueryIterGroup<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public RandomQueryIterGroup() {
        // TODO
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return null;
    }
}
