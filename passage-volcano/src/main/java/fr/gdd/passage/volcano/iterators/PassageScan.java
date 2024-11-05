package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
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
    final Op0 op;
    final Backend<ID, VALUE, Long> backend;
    final BackendIterator<ID, VALUE, Long> wrapped;
    final Tuple<Var> vars; // needed to create bindings var -> value
    final PassageExecutionContext<ID,VALUE> context;

    public PassageScan(ExecutionContext context, Op0 tripleOrQuad, Tuple<ID> spoc, BackendIterator<ID, VALUE, Long> wrapped) {
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.deadline = context.getContext().getLong(PassageConstants.DEADLINE, Long.MAX_VALUE);
        this.backend = context.getContext().get(BackendConstants.BACKEND);
        this.wrapped = wrapped;
        this.op = tripleOrQuad;

        this.vars = switch (op) {
            case OpTriple opTriple -> TupleFactory.create3(
                    opTriple.getTriple().getSubject().isVariable() && Objects.isNull(spoc.get(SPOC.SUBJECT)) ? Var.alloc(opTriple.getTriple().getSubject()) : null,
                    opTriple.getTriple().getPredicate().isVariable() && Objects.isNull(spoc.get(SPOC.PREDICATE)) ? Var.alloc(opTriple.getTriple().getPredicate()) : null,
                    opTriple.getTriple().getObject().isVariable() && Objects.isNull(spoc.get(SPOC.OBJECT)) ? Var.alloc(opTriple.getTriple().getObject()) : null);
            case OpQuad opQuad -> TupleFactory.create4(
                    opQuad.getQuad().getSubject().isVariable() && Objects.isNull(spoc.get(SPOC.SUBJECT)) ? Var.alloc(opQuad.getQuad().getSubject()) : null,
                    opQuad.getQuad().getPredicate().isVariable() && Objects.isNull(spoc.get(SPOC.PREDICATE)) ? Var.alloc(opQuad.getQuad().getPredicate()) : null,
                    opQuad.getQuad().getObject().isVariable() && Objects.isNull(spoc.get(SPOC.OBJECT)) ? Var.alloc(opQuad.getQuad().getObject()) : null,
                    opQuad.getQuad().getGraph().isVariable() && Objects.isNull(spoc.get(SPOC.GRAPH)) ? Var.alloc(opQuad.getQuad().getGraph()) : null);
            default -> throw new UnsupportedOperationException("Operator unknown: " + op);
        };
    }

    @Override
    public boolean hasNext() {
        boolean result = wrapped.hasNext();

        if (result && !context.paused.isPaused() && stopping.apply(context)) {
            // execution stops immediately, caught by {@link PassageRoot}
            throw new PauseException(op);
        }

        return result;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        wrapped.next();

        ((AtomicLong) context.getContext().get(PassageConstants.SCANS)).getAndIncrement();

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
        if (vars.len() > 3 && Objects.nonNull(vars.get(SPOC.GRAPH))) {
            newBinding.put(vars.get(SPOC.GRAPH), wrapped.getId(SPOC.GRAPH), backend).setCode(vars.get(SPOC.GRAPH), SPOC.GRAPH);
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
