package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.exceptions.BackjumpException;
import fr.gdd.passage.volcano.exceptions.PauseException;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.Arrays;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

public class SpliteratorRawScan<ID,VALUE> implements PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final Backend<ID,VALUE> backend;
    final BackendCache<ID,VALUE> cache;
    final Op0 op;
    final Long id; // The identifier is actually the starting offset

    Long offset;
    Long limit;

    BackendIterator<ID, VALUE> wrapped;
    Tuple<Var> vars; // needed to create bindings var -> value

    public SpliteratorRawScan(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpTriple opTriple) {
        this.context = context;
        this.backend = this.context.backend;
        this.cache = this.context.cache;
        this.op = opTriple;
        this.input = input;

        try {
            Tuple<ID> spo = Substitutor.substitute(opTriple.getTriple(), input, cache);
            this.vars = TupleFactory.create3(
                    opTriple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(SPOC.SUBJECT)) ? Var.alloc(opTriple.getTriple().getSubject()) : null,
                    opTriple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(SPOC.PREDICATE)) ? Var.alloc(opTriple.getTriple().getPredicate()) : null,
                    opTriple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(SPOC.OBJECT)) ? Var.alloc(opTriple.getTriple().getObject()) : null);
            this.wrapped = backend.search(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT));
        } catch (NotFoundException | IllegalArgumentException e) {
            this.vars = null;
            this.wrapped = null;
        }

        if (Objects.nonNull(wrapped) && !wrapped.hasNext()) { this.wrapped = null; }
        if (Objects.isNull(this.wrapped)) {throw new BackjumpException(op, input);}

        this.limit = this.context.getLimit(); // if null, stays null
        this.offset = Objects.nonNull(this.context.getOffset()) ? this.context.getOffset() : 0;
        this.id = this.offset;

        if (Objects.nonNull(wrapped) && offset > 0) wrapped.skip(offset); // quick skip (useful when bucketing)
    }

    public SpliteratorRawScan(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpQuad opQuad) {
        this.context = context;
        this.backend = this.context.backend;
        this.cache = this.context.cache;
        this.op = opQuad;
        this.input = input;

        try {
            Tuple<ID> spoc = Substitutor.substitute(opQuad.getQuad(), input, cache);
            this.vars = TupleFactory.create4(
                    opQuad.getQuad().getSubject().isVariable() && Objects.isNull(spoc.get(SPOC.SUBJECT)) ? Var.alloc(opQuad.getQuad().getSubject()) : null,
                    opQuad.getQuad().getPredicate().isVariable() && Objects.isNull(spoc.get(SPOC.PREDICATE)) ? Var.alloc(opQuad.getQuad().getPredicate()) : null,
                    opQuad.getQuad().getObject().isVariable() && Objects.isNull(spoc.get(SPOC.OBJECT)) ? Var.alloc(opQuad.getQuad().getObject()) : null,
                    opQuad.getQuad().getGraph().isVariable() && Objects.isNull(spoc.get(SPOC.GRAPH)) ? Var.alloc(opQuad.getQuad().getGraph()) : null);
            this.wrapped = backend.search(spoc.get(SPOC.SUBJECT), spoc.get(SPOC.PREDICATE), spoc.get(SPOC.OBJECT), spoc.get(SPOC.GRAPH));
        } catch (NotFoundException | IllegalArgumentException e) {
            this.vars = null;
            this.wrapped = null;
        }

        if (Objects.nonNull(wrapped) && !wrapped.hasNext()) { this.wrapped = null; }
        if (Objects.isNull(this.wrapped)) {throw new BackjumpException(op, input);}

        this.limit = this.context.getLimit(); // if null, stays null
        this.offset = Objects.nonNull(this.context.getOffset()) ? this.context.getOffset() : 0;
        this.id = this.offset;

        if (Objects.nonNull(wrapped) && offset > 0) wrapped.skip(offset); // quick skip (useful when bucketing)
    }

    @Override
    public Op pause() {
        return op; // nothing changes…
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        // The stopping condition are the only factor that can stop the random iterator
        // of randomly iterating.
        if (Objects.isNull(wrapped)) {
            if (context.maxScans != Long.MAX_VALUE) { context.scans.getAndIncrement(); } // even failure are counted
            return false;
        }
        if (Objects.nonNull(limit) && limit == 0) { return false; } // we produced all

        if (!context.paused.isPaused() && context.stoppingCondition.apply(context)) { // unless we must stop
            throw new PauseException(op); // execution stops immediately, caught at the root
        }

        // offset do not move.
        if (Objects.nonNull(limit)) { limit -= 1 ; } // TODO maybe force a limit ?

        // use the underlying iterator to get a new random value
        double proba = wrapped.random(); // TODO what to do about the proba?…
        wrapped.hasNext();
        wrapped.next();

        if (context.maxScans != Long.MAX_VALUE) { context.scans.getAndIncrement(); } // don't even try if not useful

        BackendBindings<ID, VALUE> newBinding = context.bindingsFactory.get();
        Arrays.stream(SPOC.spoc).forEach(code -> registerMapping(newBinding, code));

        action.accept(newBinding.setParent(input));

        return true;
    }

    /**
     * Utils to register a mapping key->value in the binding being constructed.
     * @param newBinding The mapping being constructed.
     * @param code The SPOC code of the value to add, e.g., SPOC.SUBJECT.
     */
    public void registerMapping(BackendBindings<ID,VALUE> newBinding, int code) {
        if ((code != SPOC.GRAPH && Objects.nonNull(vars.get(code))) || // spo
                (code == SPOC.GRAPH && vars.len() > 3 && Objects.nonNull(vars.get(code)))) { // spoc
            newBinding.put(vars.get(code), wrapped.getId(code), context.backend).setCode(vars.get(code), code);
        }
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        // TODO simple clone with dividing limit
        //      or bucketing strategy where OFFSETs are changed
        return null; //  for now, we don't authorize splitting
    }

    @Override
    public long estimateSize() {
        return (long) wrapped.cardinality();
    }

    @Override
    public int characteristics() {
        return 0; // TODO ?
    }

    public Long getOffset() { return offset; }
    public Long getLimit() { return limit; }
}
