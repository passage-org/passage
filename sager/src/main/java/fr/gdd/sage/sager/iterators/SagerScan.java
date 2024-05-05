package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.pause.FullyPreempted;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import fr.gdd.sage.sager.resume.BGP2Triples;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class SagerScan implements Iterator<BindingId2Value> {

    final Long deadline;
    protected final BackendIterator<NodeId, Node, ?> wrapped;
    boolean first = true;
    final OpTriple op;
    BindingId2Value current;
    final Save2SPARQL saver;
    final protected JenaBackend backend;

    final protected Tuple3<Var> vars;

    public SagerScan(ExecutionContext context, OpTriple op, Tuple<NodeId> spo, BackendIterator<NodeId, Node, ?> wrapped) {
        this.deadline = context.getContext().getLong(SagerConstants.DEADLINE, Long.MAX_VALUE);
        this.backend = context.getContext().get(SagerConstants.BACKEND);
        this.wrapped = wrapped;
        this.op = op;
        this.saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(op, this);

        vars = TupleFactory.create3(
                op.getTriple().getSubject().isVariable() && NodeId.isAny(spo.get(0)) ? Var.alloc(op.getTriple().getSubject()) : null,
                op.getTriple().getPredicate().isVariable() && NodeId.isAny(spo.get(1)) ? Var.alloc(op.getTriple().getPredicate()) : null,
                op.getTriple().getObject().isVariable() && NodeId.isAny(spo.get(2)) ? Var.alloc(op.getTriple().getObject()) : null);
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
    public BindingId2Value next() {
        first = false; // at least one iteration
        wrapped.next();

        current = new BindingId2Value().setDefaultTable(backend.getNodeTripleTable()); // TODO quads

        if (Objects.nonNull(vars.get(0))) { // ugly x3
            current.put(vars.get(0), wrapped.getId(SPOC.SUBJECT));
        }
        if (Objects.nonNull(vars.get(1))) {
            current.put(vars.get(1), wrapped.getId(SPOC.PREDICATE));
        }
        if (Objects.nonNull(vars.get(2))) {
            current.put(vars.get(2), wrapped.getId(SPOC.OBJECT));
        }

        return current;
    }

    public BindingId2Value current() {
        return this.current;
    }

    public SagerScan skip(Long offset) {
        // TODO for now, poor complexity, replace it with logarithmic skip
        long i = 0;
        while (i < offset) {
            wrapped.hasNext();
            wrapped.next();
            ++i;
        }
        return this;
    }

    public Long offset() {
        return getProgressJenaIterator().getOffset();
    }

    public Op asOpTriple() {
        /* List<Op> extendList = new ArrayList<>();
        if (Objects.isNull(vars.get(0)) && op.getTriple().getSubject().isVariable()) {
            extendList.add(FullyPreempted.toBind(current.get(op.getTriple().getSubject().getName()), Var.alloc(op.getTriple().getSubject().getName())));
        }
        if (Objects.isNull(vars.get(1)) && op.getTriple().getPredicate().isVariable()) {
            extendList.add(FullyPreempted.toBind(current.get(op.getTriple().getPredicate().getName()), Var.alloc(op.getTriple().getPredicate().getName())));
        }
        if (Objects.isNull(vars.get(2)) && op.getTriple().getObject().isVariable()) {
            extendList.add(FullyPreempted.toBind(current.get(op.getTriple().getObject().getName()), Var.alloc(op.getTriple().getObject().getName())));
        }

        Op extendJoined = BGP2Triples.asJoins(extendList);

        if (Objects.isNull(extendJoined)) {
            return op;
        } else {
            return OpJoin.create(extendJoined, op);
        }*/

        Triple t = new Triple(Objects.nonNull(vars.get(0)) ? op.getTriple().getSubject(): // true variable
                    op.getTriple().getSubject().isVariable() ? current.get(op.getTriple().getSubject().getName()): // bounded variable
                            op.getTriple().getSubject(),
                Objects.nonNull(vars.get(1)) ? op.getTriple().getPredicate(): // true variable
                        op.getTriple().getPredicate().isVariable() ? current.get(op.getTriple().getPredicate().getName()): // bounded variable
                                op.getTriple().getPredicate(),
                Objects.nonNull(vars.get(2)) ? op.getTriple().getObject(): // true variable
                        op.getTriple().getPredicate().isVariable() ? current.get(op.getTriple().getObject().getName()): // bounded variable
                                op.getTriple().getObject());
        return new OpTriple(t);
    }

    protected ProgressJenaIterator getProgressJenaIterator() {
        // TODO fix it, this is a bit ugly
        return (ProgressJenaIterator)((LazyIterator) this.wrapped).iterator;
    }

}
