package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.Substitutor;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public class SagerScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Long skip; // offset
    final Backend<ID, VALUE, Long> backend;
    final ExecutionContext context;
    final SagerOptimizer loader;
    final OpTriple triple;

    final Iterator<BackendBindings<ID, VALUE>> input;
    BackendBindings<ID, VALUE> binding;

    Iterator<BackendBindings<ID, VALUE>> instantiated;

    public SagerScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple) {
        this.input = input;
        this.triple = triple;
        instantiated = Iter.empty();
        backend = context.getContext().get(SagerConstants.BACKEND);
        loader = context.getContext().get(SagerConstants.LOADER);
        this.context = context;
        this.skip = 0L;
    }

    public SagerScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple, Long skip) {
        this.input = input;
        this.triple = triple;
        instantiated = Iter.empty();
        backend = context.getContext().get(SagerConstants.BACKEND);
        loader = context.getContext().get(SagerConstants.LOADER);
        this.context = context;
        this.skip = skip;
    }

    @Override
    public boolean hasNext() {
        if (!instantiated.hasNext() && !input.hasNext()) {
            return false;
        } else while (!instantiated.hasNext() && input.hasNext()) {
            binding = input.next();
            Tuple3<ID> spo = Substitutor.substitute(backend, triple.getTriple(), binding);

            instantiated = new SagerScan<>(context, triple, spo, backend.search(spo.get(0), spo.get(1), spo.get(2)))
                    .skip(skip);
        }

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return instantiated.next().setParent(binding);
    }

}
