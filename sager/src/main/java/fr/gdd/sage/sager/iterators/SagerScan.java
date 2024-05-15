package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.Objects;

public class SagerScan<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Long deadline;
    final OpTriple op;
    final Save2SPARQL<ID, VALUE> saver;
    final Backend<ID, VALUE, Long> backend;
    final BackendIterator<ID, VALUE, Long> wrapped;
    final protected Tuple3<Var> vars; // needed to create bindings

    boolean first = true;

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

        if (Objects.nonNull(vars.get(SPOC.SUBJECT))) { // ugly x3
            newBinding.put(vars.get(SPOC.SUBJECT), wrapped.getId(SPOC.SUBJECT), backend).setCode(vars.get(SPOC.SUBJECT), SPOC.SUBJECT);
        }
        if (Objects.nonNull(vars.get(SPOC.PREDICATE))) {
            newBinding.put(vars.get(SPOC.PREDICATE), wrapped.getId(SPOC.PREDICATE), backend).setCode(vars.get(SPOC.PREDICATE), SPOC.PREDICATE);
        }
        if (Objects.nonNull(vars.get(SPOC.OBJECT))) {
            newBinding.put(vars.get(SPOC.OBJECT), wrapped.getId(SPOC.OBJECT), backend).setCode(vars.get(SPOC.OBJECT), SPOC.OBJECT);
        }

        return newBinding;
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

    /**
     * @return The iterator's current state in form of `Bind As` clause.
     */
    public Op asBindAs () {
        OpSequence seq = OpSequence.create();
        if (Objects.nonNull(vars.get(SPOC.SUBJECT))) { // ugly x3
            seq.add(OpExtend.extend(OpTable.unit(), vars.get(SPOC.SUBJECT), ExprUtils.parse(wrapped.getString(SPOC.SUBJECT)))); // ExprUtils.parse(NodeFmtLib.displayStr(wrapped.getString(SPOC.SUBJECT)))););
        }
        if (Objects.nonNull(vars.get(SPOC.PREDICATE))) {
            seq.add(OpExtend.extend(OpTable.unit(), vars.get(SPOC.PREDICATE), ExprUtils.parse(wrapped.getString(SPOC.PREDICATE))));
        }
        if (Objects.nonNull(vars.get(SPOC.OBJECT))) {
            seq.add(OpExtend.extend(OpTable.unit(), vars.get(SPOC.OBJECT), ExprUtils.parse(wrapped.getString(SPOC.OBJECT))));
        }
        return seq.size() > 1 ? seq : seq.get(0); // remove sequence if not needed
    }

}
