package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.Pause2SPARQL;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

public class PassageScan<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    /**
     * By default, this is based on execution time. However, developers can change it
     * e.g., for testing purposes.
     */
    public static Function<ExecutionContext, Boolean> stopping = (ec) ->
            // ec.getContext().getLong(SagerConstants.SCANS, Long.MAX_VALUE) > 1 &&
            System.currentTimeMillis() >= ec.getContext().getLong(PassageConstants.DEADLINE, Long.MAX_VALUE);

    final Long deadline;
    final OpTriple op;
    final Pause2SPARQL<ID, VALUE> saver;
    final Backend<ID, VALUE, Long> backend;
    final BackendIterator<ID, VALUE, Long> wrapped;
    final Tuple3<Var> vars; // needed to create bindings
    final ExecutionContext context;

    public PassageScan(ExecutionContext context, OpTriple triple, Tuple<ID> spo, BackendIterator<ID, VALUE, Long> wrapped) {
        this.context = context;
        this.deadline = context.getContext().getLong(PassageConstants.DEADLINE, Long.MAX_VALUE);
        this.backend = context.getContext().get(PassageConstants.BACKEND);
        this.wrapped = wrapped;
        this.op = triple;
        this.saver = context.getContext().get(PassageConstants.SAVER);
        // saver.register(triple, this);

        this.vars = TupleFactory.create3(
                triple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(0)) ? Var.alloc(triple.getTriple().getSubject()) : null,
                triple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(1)) ? Var.alloc(triple.getTriple().getPredicate()) : null,
                triple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(2)) ? Var.alloc(triple.getTriple().getObject()) : null);
    }

    @Override
    public boolean hasNext() {
        boolean result = wrapped.hasNext();

        if (result && context.getContext().isFalse(PassageConstants.PAUSED) && stopping.apply(context)) {
            // execution stops immediately, caught by {@link PreemptRootIter}
            throw new PauseException(op);
        }

        return result;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        // first = false; // at least one iteration
        wrapped.next();

        context.getContext().set(PassageConstants.SCANS, context.getContext().getLong(PassageConstants.SCANS,0L) + 1);

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

    public PassageScan<ID, VALUE> skip(Long offset) {
        wrapped.skip(offset);
        return this;
    }

    public Long previous() {
        return wrapped.previous();
    }

    public Long current() {
        return wrapped.current();
    }

    public double cardinality () {
        return wrapped.cardinality();
    }

}
