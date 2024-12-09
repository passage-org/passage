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
import fr.gdd.passage.volcano.exceptions.BackjumpException;
import fr.gdd.passage.volcano.exceptions.PauseException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.VarUtils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class PassageSplitScan<ID,VALUE> extends PausableSpliterator<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>> {

    /**
     * By default, this is based on execution time. However, developers can change it
     * e.g., for testing purposes.
     */
    public volatile static Function<PassageExecutionContext, Boolean> stopping = (ec) ->
            System.currentTimeMillis() >= ec.getDeadline() || ec.scans.get() >= ec.maxScans;

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final Backend<ID,VALUE> backend;
    final BackendCache<ID,VALUE> cache;
    final Op0 op;
    Set<Var> producedVars;
    Set<Var> consumedVars;

    Long offset;
    Long limit;

    BackendIterator<ID, VALUE> wrapped;
    Tuple<Var> vars; // needed to create bindings var -> value

    public PassageSplitScan (ExecutionContext context, BackendBindings<ID,VALUE> input, Op0 tripleOrQuad) {
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

        if (Objects.nonNull(wrapped)) {
            if (!wrapped.hasNext()) {
                wrapped = null;
            }
        }

        if (this.context.backjump && Objects.isNull(wrapped)) { // no throw, no overhead
            unregister();
            getLazyProducedConsumed(input);
            // throws immediately if there are no results, no need to try advance
            if (!consumedVars.isEmpty()) {
                throw new BackjumpException(consumedVars);
            }
        }

        this.limit = this.context.getLimit(); // if null, stays null
        this.offset = Objects.nonNull(this.context.getOffset()) ? this.context.getOffset() : 0;
        if (Objects.nonNull(wrapped) && offset > 0) wrapped.skip(offset); // quick skip
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.isNull(wrapped)) { unregister(); return false; }
        if (Objects.nonNull(limit) && limit == 0) { unregister(); return false; } // we produced all

        if (wrapped.hasNext()) { // actually iterates over the dataset
            if (!context.paused.isPaused() && stopping.apply(context)) { // unless we must stop
                // execution stops immediately, caught by {@link PassageRoot}
                throw new PauseException(op);
            }

            // but if not pause, we create the new binding
            offset += 1;
            if (Objects.nonNull(limit)) { limit -= 1 ; }
            wrapped.next();
            context.scans.getAndIncrement();
            BackendBindings<ID, VALUE> newBinding = new BackendBindings<>();

            if (Objects.nonNull(vars.get(SPOC.SUBJECT))) { // ugly x4
                newBinding.put(vars.get(SPOC.SUBJECT), wrapped.getId(SPOC.SUBJECT), context.backend).setCode(vars.get(SPOC.SUBJECT), SPOC.SUBJECT);
            }
            if (Objects.nonNull(vars.get(SPOC.PREDICATE))) {
                newBinding.put(vars.get(SPOC.PREDICATE), wrapped.getId(SPOC.PREDICATE), context.backend).setCode(vars.get(SPOC.PREDICATE), SPOC.PREDICATE);
            }
            if (Objects.nonNull(vars.get(SPOC.OBJECT))) {
                newBinding.put(vars.get(SPOC.OBJECT), wrapped.getId(SPOC.OBJECT), context.backend).setCode(vars.get(SPOC.OBJECT), SPOC.OBJECT);
            }
            if (vars.len() > 3 && Objects.nonNull(vars.get(SPOC.GRAPH))) {
                newBinding.put(vars.get(SPOC.GRAPH), wrapped.getId(SPOC.GRAPH), context.backend).setCode(vars.get(SPOC.GRAPH), SPOC.GRAPH);
            }

            try {
                action.accept(newBinding.setParent(input));
            } catch (BackjumpException bje) {
                getLazyProducedConsumed(input);
                if (producedVars.stream().noneMatch(bje.problematicVariables::contains)) {
                    unregister();
                    throw bje; // forward the exception
                }
            }
            return true;
        }

        unregister();
        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        long remaining = estimateSize();
        if (remaining < 2) { return null; } // no split possible
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

        return new PassageSplitScan<>(newContext, input, op);
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
        if (Objects.nonNull(limit) && limit == 0) return OpTable.empty(); // done
        // save the whole context
        Op toSave = OpJoin.create(input.toOp(), op);
        // update LIMIT and OFFSET
        long newLimit = Objects.isNull(limit) ? Long.MIN_VALUE : limit;
        long newOffset = Objects.isNull(offset) || offset == 0 ? Long.MIN_VALUE : offset; // to simplify the query
        if (newLimit == Long.MIN_VALUE && newOffset == Long.MIN_VALUE) {
            return toSave;
        } else { // if either LIMIT or OFFSET, we need to create a subquery
            return new OpSlice(toSave, newOffset, newLimit);
        }
    }

    /* *********************************** UTILS ************************************ */

    private void getLazyProducedConsumed(BackendBindings<ID,VALUE> input) {
        if (Objects.nonNull(this.producedVars)) { return ; }
        this.producedVars = switch (op) {
            case OpTriple triple -> {
                Set<Var> _producedVars = VarUtils.getVars(triple.getTriple());
                _producedVars.removeAll(input.variables());
                yield _producedVars;
            }
            case OpQuad quad -> {
                Set<Var> _producedVars = VarUtils.getVars(quad.getQuad().asTriple());
                if (quad.getQuad().getGraph() instanceof Var _v) {
                    _producedVars.add(_v);
                }
                _producedVars.removeAll(input.variables());
                yield _producedVars;
            }
            case OpTable table -> {
                Set<Var> _producedVars = new HashSet<>(table.getTable().getVars());
                _producedVars.removeAll(input.variables());
                yield _producedVars;
            }
            default -> Set.of();
        };
        this.consumedVars = switch (op) {
            case OpTriple triple -> {
                Set<Var> _consumedVars = VarUtils.getVars(triple.getTriple());
                _consumedVars.removeAll(producedVars);
                yield _consumedVars;
            }
            default -> Set.of();
        };
    }


    public Long getOffset() {
        return offset;
    }

    public Long getLimit() {
        return limit;
    }
}
