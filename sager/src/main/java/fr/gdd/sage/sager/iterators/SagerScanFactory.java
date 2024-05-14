package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;
import java.util.Objects;

public class SagerScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    JenaBackend backend;
    ExecutionContext context;
    SagerOptimizer loader;

    OpTriple triple;
    final Long skip; // offset
    Iterator<BackendBindings<ID, VALUE>> input;
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
            Tuple3<NodeId> spo = substitute(triple.getTriple(), binding);

            instantiated = new SagerScan(context,
                    triple,
                    spo,
                    backend.search(spo.get(0), spo.get(1), spo.get(2))).skip(skip);
        }

        return instantiated.hasNext();
    }

    @Override
    public BindingId2Value next() {
        return instantiated.next().setParent(binding);
    }

    protected Tuple3<NodeId> substitute(Triple triple, BindingId2Value binding) {
        return TupleFactory.create3(substitute(triple.getSubject(), binding),
                substitute(triple.getPredicate(),binding),
                substitute(triple.getObject(), binding));
    }

    protected NodeId substitute(Node sOrPOrO, BindingId2Value binding) {
        if (sOrPOrO.isVariable()) {
            NodeId id = binding.getId(Var.alloc(sOrPOrO), backend.getNodeTripleTable());
            return Objects.isNull(id) ? NodeId.NodeIdAny : id;
        } else {
            return backend.getId(sOrPOrO);
        }
    }

    /* ************************************************************ */

    protected Iterator<BindingId2Value> getInstantiated() {
        return instantiated;
    }

    protected Iterator<BindingId2Value> getInput() {
        return input;
    }

    protected void setBinding(BindingId2Value binding) {
        this.binding = binding;
    }

    protected BindingId2Value getBinding() {
        return binding;
    }

    protected OpTriple getTriple() {
        return triple;
    }

    protected void setInstantiated(Iterator<BindingId2Value> instantiated) {
        this.instantiated = instantiated;
    }

    protected ExecutionContext getContext() {
        return context;
    }

    protected JenaBackend getBackend() {
        return backend;
    }
}
