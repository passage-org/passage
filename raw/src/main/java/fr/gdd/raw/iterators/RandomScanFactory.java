package fr.gdd.raw.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawConstants;
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
    final BackendCache<ID,VALUE> cache;

    BackendBindings<ID, VALUE> inputBinding;
    Iterator<BackendBindings<ID, VALUE>> instantiated = Iter.empty();


    public RandomScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple) {
        this.input = input;
        this.context = context;
        this.triple = triple;
        this.backend = context.getContext().get(RawConstants.BACKEND);
        this.cache = context.getContext().get(RawConstants.CACHE);
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
