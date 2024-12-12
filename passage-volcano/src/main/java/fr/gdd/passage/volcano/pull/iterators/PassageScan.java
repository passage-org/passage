package fr.gdd.passage.volcano.pull.iterators;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendQuadsFactory;
import fr.gdd.passage.commons.factories.IBackendTriplesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.commons.iterators.BackendIteratorOverInput;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.exceptions.PauseException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * The most default basic simple iterator that scans the dataset.
 */
public class PassageScan<ID, VALUE> extends PausableIterator<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>>{

    public static <ID,VALUE> IBackendTriplesFactory<ID,VALUE> triplesFactory() {
        return (context, input, op) -> new BackendIteratorOverInput<>(context, input, (Op0) op, PassageScan::new);
    }

    public static <ID,VALUE> IBackendQuadsFactory<ID,VALUE> quadsFactory() {
        return (context, input, op) -> new BackendIteratorOverInput<>(context, input, (Op0) op, PassageScan::new);
    }

    /**
     * By default, this is based on execution time. However, developers can change it
     * e.g., for testing purposes.
     */
    public volatile static Function<PassageExecutionContext, Boolean> stopping = (ec) ->
            System.currentTimeMillis() >= ec.getDeadline() || ec.scans.get() >= ec.maxScans;

    final Backend<ID,VALUE> backend;
    final BackendCache<ID,VALUE> cache;
    final Long deadline;
    final Op0 op;

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final Long limit;
    final Long offset;

    BackendIterator<ID, VALUE> wrapped;
    Tuple<Var> vars; // needed to create bindings var -> value
    Long produced = 0L;
    boolean consumed = true;

    public PassageScan(ExecutionContext context, BackendBindings<ID,VALUE> input, Op0 tripleOrQuad) {
        super((PassageExecutionContext<ID, VALUE>) context, tripleOrQuad);
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.backend = this.context.backend;
        this.cache = this.context.cache;
        this.op = tripleOrQuad;
        this.input = input;

        try {
            switch (tripleOrQuad) {
                case OpTriple opTriple -> {
                    Tuple<ID> spo = Substitutor.substitute(opTriple.getTriple(), input, cache);
                    this.vars = TupleFactory.create3(
                            opTriple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(SPOC.SUBJECT)) ? Var.alloc(opTriple.getTriple().getSubject()) : null,
                            opTriple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(SPOC.PREDICATE)) ? Var.alloc(opTriple.getTriple().getPredicate()) : null,
                            opTriple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(SPOC.OBJECT)) ? Var.alloc(opTriple.getTriple().getObject()) : null);
                    this.wrapped = backend.search(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT));
                }
                case OpQuad opQuad -> {
                    Tuple<ID> spoc = Substitutor.substitute(opQuad.getQuad(), input, cache);
                    this.vars = TupleFactory.create4(
                            opQuad.getQuad().getSubject().isVariable() && Objects.isNull(spoc.get(SPOC.SUBJECT)) ? Var.alloc(opQuad.getQuad().getSubject()) : null,
                            opQuad.getQuad().getPredicate().isVariable() && Objects.isNull(spoc.get(SPOC.PREDICATE)) ? Var.alloc(opQuad.getQuad().getPredicate()) : null,
                            opQuad.getQuad().getObject().isVariable() && Objects.isNull(spoc.get(SPOC.OBJECT)) ? Var.alloc(opQuad.getQuad().getObject()) : null,
                            opQuad.getQuad().getGraph().isVariable() && Objects.isNull(spoc.get(SPOC.GRAPH)) ? Var.alloc(opQuad.getQuad().getGraph()) : null);
                    this.wrapped = backend.search(spoc.get(SPOC.SUBJECT), spoc.get(SPOC.PREDICATE), spoc.get(SPOC.OBJECT), spoc.get(SPOC.GRAPH));
                }
                default -> throw new UnsupportedOperationException("Operator not handle here: " + tripleOrQuad);
            }
        } catch (NotFoundException | IllegalArgumentException e) {
            this.vars = null;
            this.wrapped = null;
        }

        this.deadline = this.context.getDeadline();
        this.limit = this.context.getLimit();
        this.offset = this.context.getOffset();
        if (Objects.nonNull(wrapped) && Objects.nonNull(offset) && offset > 0) wrapped.skip(offset); // quick skip
    }

    @Override
    public boolean hasNext() {
        if (Objects.isNull(wrapped)) {return false;}
        if (!consumed) {return true;}
        if (Objects.nonNull(limit) && produced >= limit) return false;

        boolean result = wrapped.hasNext();

        if (result && !context.paused.isPaused() && stopping.apply(context)) {
            // execution stops immediately, caught by {@link PassageRoot}
            throw new PauseException(op);
        }

        consumed = !result;

        return result;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        consumed = true;
        produced += 1;
        wrapped.next();

        context.scans.incrementAndGet();

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

    public double cardinality(){
        if (Objects.nonNull(wrapped) && this.wrapped.hasNext()) {
            return this.wrapped.cardinality();
        }
        return 0.;
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

        Op toSave = OpJoin.create(input.toOp(), op);
        long newLimit = Objects.isNull(limit) || limit == Long.MIN_VALUE ? Long.MIN_VALUE : limit - produced;
        return new OpSlice(toSave, wrapped.current(), newLimit);
    }
}
