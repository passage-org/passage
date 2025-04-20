package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.exceptions.PauseException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.Arrays;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static fr.gdd.passage.volcano.push.Pause2Continuation.DONE;
import static fr.gdd.passage.volcano.push.Pause2Continuation.removeEmptyOfUnion;

/**
 * A basic scan with the ability to divide the space to explore. The virtual
 * SPARQL query is then a union of partitions represented through `LIMIT` and `OFFSET`
 * clauses.
 */
public class SpliteratorScan<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>>, PausableSpliterator<ID,VALUE> {

    protected final PassageExecutionContext<ID,VALUE> context;
    protected final BackendBindings<ID,VALUE> input;
    protected final Backend<ID,VALUE> backend;
    protected final BackendCache<ID,VALUE> cache;
    protected final Op0 op;
    protected final Long id; // The identifier is actually the starting offset
    protected ConcurrentHashMap<Long, SpliteratorScan<ID,VALUE>> siblings = new ConcurrentHashMap<>(); // offset -> sibling

    protected Long offset;
    protected Long limit;

    protected BackendIterator<ID, VALUE> wrapped;
    protected Tuple<Var> vars; // needed to create bindings var -> value

    public SpliteratorScan(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpTriple triple) {
        this.context = context;
        this.backend = this.context.backend;
        this.cache = this.context.cache;
        this.op = triple;
        this.input = input;

        try {
            Tuple<ID> spo = Substitutor.substitute(triple.getTriple(), input, cache);
            this.vars = TupleFactory.create3(
                    triple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(SPOC.SUBJECT)) ? Var.alloc(triple.getTriple().getSubject()) : null,
                    triple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(SPOC.PREDICATE)) ? Var.alloc(triple.getTriple().getPredicate()) : null,
                    triple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(SPOC.OBJECT)) ? Var.alloc(triple.getTriple().getObject()) : null);
            this.wrapped = backend.search(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT));
        } catch (NotFoundException | IllegalArgumentException e) {
            this.vars = null;
            this.wrapped = null;
        }

        if (Objects.nonNull(wrapped) && !wrapped.hasNext()) { wrapped = null; }

        this.offset = Objects.nonNull(this.context.getOffset()) ? this.context.getOffset() : 0;
        this.id = this.offset;
        this.siblings.put(id, this);

        this.limit = this.context.getLimit(); // if null, stays null

        if (Objects.nonNull(wrapped) && offset > 0) wrapped.skip(offset); // quick skip
    }

    public SpliteratorScan(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpQuad quad) {
        this.context = context;
        this.backend = this.context.backend;
        this.cache = this.context.cache;
        this.op = quad;
        this.input = input;

        try {
            Tuple<ID> spoc = Substitutor.substitute(quad.getQuad(), input, cache);
            this.vars = TupleFactory.create4(
                    quad.getQuad().getSubject().isVariable() && Objects.isNull(spoc.get(SPOC.SUBJECT)) ? Var.alloc(quad.getQuad().getSubject()) : null,
                    quad.getQuad().getPredicate().isVariable() && Objects.isNull(spoc.get(SPOC.PREDICATE)) ? Var.alloc(quad.getQuad().getPredicate()) : null,
                    quad.getQuad().getObject().isVariable() && Objects.isNull(spoc.get(SPOC.OBJECT)) ? Var.alloc(quad.getQuad().getObject()) : null,
                    quad.getQuad().getGraph().isVariable() && Objects.isNull(spoc.get(SPOC.GRAPH)) ? Var.alloc(quad.getQuad().getGraph()) : null);
            this.wrapped = backend.search(spoc.get(SPOC.SUBJECT), spoc.get(SPOC.PREDICATE), spoc.get(SPOC.OBJECT), spoc.get(SPOC.GRAPH));
        } catch (NotFoundException | IllegalArgumentException e) {
            this.vars = null;
            this.wrapped = null;
        }

        if (Objects.nonNull(wrapped) && !wrapped.hasNext()) { wrapped = null; }

        this.limit = this.context.getLimit(); // if null, stays null
        this.offset = Objects.nonNull(this.context.getOffset()) ? this.context.getOffset() : 0;
        this.id = this.offset;
        this.siblings.put(id, this);

        if (Objects.nonNull(wrapped) && offset > 0) wrapped.skip(offset); // quick skip
    }

    public Long getOffset() { return offset; }
    public Long getLimit() { return limit; }

    /**
     * When parallel, it allows keeping track of all spliterators running for this
     * quad/triple pattern.
     * @param siblings The concurrent map that registers
     */
    public SpliteratorScan<ID,VALUE> register(ConcurrentHashMap<Long, SpliteratorScan<ID, VALUE>> siblings) {
        this.siblings = siblings;
        this.siblings.put(id, this);
        return this;
    }

    public void unregister() {
        this.siblings.remove(id);
    }

    /* ******************************* SPLITERATOR *********************************** */

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.isNull(wrapped)) { unregister(); return false; }
        if (Objects.nonNull(limit) && limit == 0) { unregister(); return false; } // we produced all

        if (wrapped.hasNext()) { // actually iterates over the dataset
            if (!context.paused.isPaused() && context.stoppingConditions.stream().anyMatch(c -> c.test(context))) { // unless we must stop
                throw new PauseException(op); // execution stops immediately, caught at the root
            }

            // but if not pause, we create the new binding
            offset += 1;
            if (Objects.nonNull(limit)) { limit -= 1 ; }
            wrapped.next();
            context.incrementNbScans();

            BackendBindings<ID, VALUE> newBinding = context.bindingsFactory.get();
            Arrays.stream(SPOC.spoc).forEach(code -> registerMapping(newBinding, code));
            action.accept(newBinding.setParent(input));
            return true;
        }

        unregister();
        return false;
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
        long remaining = estimateSize();
        if (remaining < context.splitScans) {return null; } // no split possible
        // #1 limit the size of this split iterator
        long splitIndex = offset + remaining / 2;
        // #2 create another split iterator of removed size
        PassageExecutionContext<ID,VALUE> newContext =
                new PassageExecutionContextBuilder<ID,VALUE>()
                        .setContext(context).build()
                        .setOffset(splitIndex);

        if (Objects.nonNull(this.limit)) {
            newContext.setLimit(offset + remaining - splitIndex);
        }

        this.limit = splitIndex - offset;

        return switch (op) {
            case OpTriple t -> new SpliteratorScan<>(newContext, input, t).register(this.siblings);
            case OpQuad q -> new SpliteratorScan<>(newContext, input, q).register(this.siblings);
            default -> throw new IllegalStateException("Scan does not handle " + op);
        };
    }

    @Override
    public long estimateSize() { // what is left right now
        if (Objects.isNull(wrapped)) return 0;
        long cardinality = (long) wrapped.cardinality();
        return  (Objects.isNull(limit)) ? cardinality - offset : Math.min(cardinality - offset, limit);
    }

    @Override
    public int characteristics() {
        return SUBSIZED | SIZED | NONNULL | IMMUTABLE;
    }

    /* *********************************** PAUSE ************************************ */

    @Override
    public Op pause() {
        if (siblings.isEmpty()) { return DONE; }
        return siblings.values().stream().map(SpliteratorScan::pauseOne).reduce(DONE, removeEmptyOfUnion);
    }

    public Op pauseOne() {
        if (Objects.nonNull(limit) && limit == 0) return DONE;
        // save the whole context
        Op toSave = input.joinWith(op);
        // update LIMIT and OFFSET
        long newLimit = Objects.isNull(limit) ? Long.MIN_VALUE : limit;
        long newOffset = Objects.isNull(offset) || offset == 0 ? Long.MIN_VALUE : offset; // to simplify the query

        if (newLimit == Long.MIN_VALUE && newOffset == Long.MIN_VALUE) {
            return toSave;
        } else { // if either LIMIT or OFFSET, we need to create a subquery
            return new OpSlice(toSave, newOffset, newLimit);
        }
    }
}
