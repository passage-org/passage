package fr.gdd.raw.iterators;

import fr.gdd.passage.commons.factories.IBackendUnionsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public class RandomUnion<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendUnionsFactory<ID,VALUE> factory() {
        return (context, input, opUnion) -> {

            return new RandomUnion<>(context, input, opUnion);
        };
    }

    final ExecutionContext context;
    final Backend<ID,VALUE> backend;
    final BackendCache<ID,VALUE> cache;
    final OpUnion opUnion;

    BackendBindings<ID, VALUE> current;

    boolean hasProduced = false;

    public RandomUnion(ExecutionContext context, Iterator<BackendBindings<ID,VALUE>> input, OpUnion opUnion) {
        this.context = context;
        this.opUnion = opUnion;

        this.backend = this.context.getContext().get(RawConstants.BACKEND);
        this.cache = this.context.getContext().get(RawConstants.CACHE);

        if(input.hasNext()) {
            current = input.next();
        }
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        hasProduced = true;
        return null;
    }
}
