package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.generics.Substitutor;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SagerScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Long skip; // offset
    final Backend<ID, VALUE, Long> backend;
    final ExecutionContext context;
    final SagerOptimizer loader;
    final OpTriple triple;
    final CacheId<ID,VALUE> cache;

    final Iterator<BackendBindings<ID, VALUE>> input;
    BackendBindings<ID, VALUE> inputBinding;

    Iterator<BackendBindings<ID, VALUE>> instantiated;

    public SagerScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple) {
        this.input = input;
        this.triple = triple;
        instantiated = Iter.empty();
        backend = context.getContext().get(SagerConstants.BACKEND);
        loader = context.getContext().get(SagerConstants.LOADER);
        this.context = context;
        this.skip = 0L;
        Save2SPARQL<ID, VALUE> saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(triple, this);
        this.cache = context.getContext().get(SagerConstants.CACHE);
    }

    public SagerScanFactory(Iterator<BackendBindings<ID, VALUE>> input, ExecutionContext context, OpTriple triple, Long skip) {
        this.input = input;
        this.triple = triple;
        instantiated = Iter.empty();
        backend = context.getContext().get(SagerConstants.BACKEND);
        loader = context.getContext().get(SagerConstants.LOADER);
        this.context = context;
        this.skip = skip;
        Save2SPARQL<ID, VALUE> saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(triple, this);
        this.cache = context.getContext().get(SagerConstants.CACHE);
    }

    @Override
    public boolean hasNext() {
        if (!instantiated.hasNext() && !input.hasNext()) {
            return false;
        } else while (!instantiated.hasNext() && input.hasNext()) {
            inputBinding = input.next();
            Tuple3<ID> spo = Substitutor.substitute(backend, triple.getTriple(), inputBinding, cache);

            instantiated = new SagerScan<>(context, triple, spo, backend.search(spo.get(0), spo.get(1), spo.get(2)))
                    .skip(skip);
        }

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return instantiated.next().setParent(inputBinding);
    }


    public double cardinality() {
        if (instantiated instanceof SagerScan<ID,VALUE> scan) {
            return scan.cardinality();
        }
        return 0.;
    }

    public long offset() {
        if (instantiated instanceof SagerScan<ID,VALUE> scan) {
            return scan.current();
        }
        return 0L;
    }

    public Op preempt() {
        if (!instantiated.hasNext()) {
            return null;
        }

        Set<Var> vars = inputBinding.vars();
        OpSequence seq = OpSequence.create();
        for (Var v : vars) {
            seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(inputBinding.get(v).getString())));
        }
        seq.add(triple);
        return new OpSlice(seq, ((SagerScan<ID, VALUE>) instantiated).current(), Long.MIN_VALUE);
    }

}
