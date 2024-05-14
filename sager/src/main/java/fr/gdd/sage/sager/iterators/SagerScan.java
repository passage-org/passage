package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;

import java.util.Iterator;
import java.util.Objects;

public class SagerScan<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Long deadline;
    final OpTriple op;
    final Save2SPARQL saver;
    final Backend<ID, VALUE, Long> backend;
    final BackendIterator<ID, VALUE, Long> wrapped;
    final protected Tuple3<Var> vars; // needed to create bindings

    boolean first = true;
    BackendBindings<ID, VALUE> current;

    public SagerScan(ExecutionContext context, OpTriple triple, Tuple<ID> spo, BackendIterator<ID, VALUE, Long> wrapped) {
        this.deadline = context.getContext().getLong(SagerConstants.DEADLINE, Long.MAX_VALUE);
        this.backend = context.getContext().get(SagerConstants.BACKEND);
        this.wrapped = wrapped;
        this.op = triple;
        this.saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(triple, this);

        this.vars = TupleFactory.create3(
                triple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(0)) ? Var.alloc(triple.getTriple().getSubject()) : null,
                triple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(1)) ? Var.alloc(triple.getTriple().getPredicate()) : null,
                triple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(2)) ? Var.alloc(triple.getTriple().getObject()) : null);
    }

    @Override
    public boolean hasNext() {
        boolean result = wrapped.hasNext();

        if (result && !first && System.currentTimeMillis() >= deadline) {
            // execution stops immediately, caught by {@link PreemptRootIter}
            throw new PauseException(op);
        }

        if (!result) { saver.unregister(op); }

        return result;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        first = false; // at least one iteration
        wrapped.next();

        BackendBindings<ID, VALUE> newBinding = new BackendBindings<>();

        if (Objects.nonNull(vars.get(0))) { // ugly x3
            newBinding.put(vars.get(0), wrapped.getId(SPOC.SUBJECT), backend).setCode(vars.get(0), SPOC.SUBJECT);
        }
        if (Objects.nonNull(vars.get(1))) {
            newBinding.put(vars.get(1), wrapped.getId(SPOC.PREDICATE), backend).setCode(vars.get(1), SPOC.PREDICATE);
        }
        if (Objects.nonNull(vars.get(2))) {
            newBinding.put(vars.get(2), wrapped.getId(SPOC.OBJECT), backend).setCode(vars.get(2), SPOC.OBJECT);
        }

        return current;
    }

    public SagerScan<ID, VALUE> skip(Long offset) {
        wrapped.skip(offset);
        return this;
    }

    public Long previous() {
        return wrapped.previous();
    }

    public Long current() {
        return wrapped.current();
    }

}
