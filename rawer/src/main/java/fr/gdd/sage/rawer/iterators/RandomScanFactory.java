package fr.gdd.sage.rawer.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.generics.Substitutor;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public class RandomScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Backend<ID, VALUE, ?> backend;
    final Iterator<BackendBindings<ID, VALUE>> input;
    final ExecutionContext context;
    final OpTriple triple;
    final CacheId<ID,VALUE> cache;

    BackendBindings<ID, VALUE> inputBinding;
    Iterator<BackendBindings<ID, VALUE>> instantiated = Iter.empty();


    public RandomScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple) {
        this.input = input;
        this.context = context;
        this.triple = triple;
        this.backend = context.getContext().get(RawerConstants.BACKEND);
        this.cache = context.getContext().get(RawerConstants.CACHE);
    }

    @Override
    public boolean hasNext() {
        if (!instantiated.hasNext() && !input.hasNext()) {
            return false;
        } else while (!instantiated.hasNext() && input.hasNext()) {
            inputBinding = input.next();
            Tuple3<ID> spo = Substitutor.substitute(triple.getTriple(), inputBinding, cache);

            instantiated = new RandomScan<>(context, triple, spo);
        }

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return instantiated.next().setParent(inputBinding);
    }

}
