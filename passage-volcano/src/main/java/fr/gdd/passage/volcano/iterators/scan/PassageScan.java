package fr.gdd.passage.volcano.iterators.scan;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.iterators.PauseException;
import fr.gdd.passage.volcano.iterators.PausableIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The most default basic simple iterator that scans the dataset.
 */
public class PassageScan<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>>, PausableIterator {

    /**
     * By default, this is based on execution time. However, developers can change it
     * e.g., for testing purposes.
     */
    public static Function<ExecutionContext, Boolean> stopping = (ec) ->
            System.currentTimeMillis() >= ec.getContext().getLong(PassageConstants.DEADLINE, Long.MAX_VALUE);

    final Long deadline;
    final Op0 op;
    final BackendIterator<ID, VALUE, Long> wrapped;
    final Tuple<Var> vars; // needed to create bindings var -> value
    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final Long limit;
    final Long offset;

    Long produced = 0L;

    public PassageScan(ExecutionContext context, BackendBindings<ID,VALUE> input, Op0 tripleOrQuad,
                       Tuple<ID> spoc, BackendIterator<ID, VALUE, Long> wrapped, Long offset, Long limit) {
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.deadline = context.getContext().getLong(PassageConstants.DEADLINE, Long.MAX_VALUE);
        this.wrapped = wrapped;
        this.op = tripleOrQuad;
        this.input = input;
        this.limit = limit;
        this.offset = offset;

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

        if (Objects.nonNull(offset) && offset > 0) wrapped.skip(offset); // quick skip
    }

    @Override
    public boolean hasNext() {
        if (Objects.nonNull(limit) && produced >= limit) return false;

        boolean result = wrapped.hasNext();

        if (result && !context.paused.isPaused() && stopping.apply(context)) {
            // execution stops immediately, caught by {@link PassageRoot}
            throw new PauseException(op);
        }

        return result;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        produced += 1;
        wrapped.next();

        ((AtomicLong) context.getContext().get(PassageConstants.SCANS)).getAndIncrement();

        BackendBindings<ID, VALUE> newBinding = new BackendBindings<>();

        if (Objects.nonNull(vars.get(SPOC.SUBJECT))) { // ugly x3
            newBinding.put(vars.get(SPOC.SUBJECT), wrapped.getId(SPOC.SUBJECT), this.context.backend).setCode(vars.get(SPOC.SUBJECT), SPOC.SUBJECT);
        }
        if (Objects.nonNull(vars.get(SPOC.PREDICATE))) {
            newBinding.put(vars.get(SPOC.PREDICATE), wrapped.getId(SPOC.PREDICATE), this.context.backend).setCode(vars.get(SPOC.PREDICATE), SPOC.PREDICATE);
        }
        if (Objects.nonNull(vars.get(SPOC.OBJECT))) {
            newBinding.put(vars.get(SPOC.OBJECT), wrapped.getId(SPOC.OBJECT), this.context.backend).setCode(vars.get(SPOC.OBJECT), SPOC.OBJECT);
        }
        if (vars.len() > 3 && Objects.nonNull(vars.get(SPOC.GRAPH))) {
            newBinding.put(vars.get(SPOC.GRAPH), wrapped.getId(SPOC.GRAPH), this.context.backend).setCode(vars.get(SPOC.GRAPH), SPOC.GRAPH);
        }

        return newBinding.setParent(input);
    }

    /**
     * @return The Jena operator that summarizes the current state of this scan iterator.
     * It is made of `Bind … As …` to save the state that created this iterator, plus the triple pattern
     * itself unmoved, plus a slice operator that defines an offset.
     * It returns `null` when the wrapped scan iterator does not have a next binding.
     */
    @Override
    public Op pause() {
        if (!this.hasNext()) { return null; }

        Set<Var> vars = input.variables();
        OpSequence seq = OpSequence.create();
        for (Var v : vars) {
            seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(input.getBinding(v).getString())));
        }
        seq.add(op);

        Op seqOrSingle = seq.size() > 1 ? seq : seq.get(0);
        long newLimit = Objects.isNull(limit) || limit == Long.MIN_VALUE ? Long.MIN_VALUE : limit - produced;
        return new OpSlice(seqOrSingle, wrapped.current(), newLimit);
    }
}
