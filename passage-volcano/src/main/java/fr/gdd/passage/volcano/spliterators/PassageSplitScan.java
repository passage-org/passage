package fr.gdd.passage.volcano.spliterators;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.pause.PauseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.VarUtils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class PassageSplitScan<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>> {

    /**
     * By default, this is based on execution time. However, developers can change it
     * e.g., for testing purposes.
     */
    public static Function<ExecutionContext, Boolean> stopping = (ec) ->
            System.currentTimeMillis() >= ec.getContext().getLong(PassageConstants.DEADLINE, Long.MAX_VALUE);

    final Backend<ID,VALUE,Long> backend;
    final BackendCache<ID,VALUE> cache;
    final Long deadline;
    final Op0 op;

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;

    final Long offset;
    Long limit;

    BackendIterator<ID, VALUE, Long> wrapped;
    Tuple<Var> vars; // needed to create bindings var -> value
    Long produced = 0L;

    public PassageSplitScan (ExecutionContext context, BackendBindings<ID,VALUE> input, Op0 tripleOrQuad) {
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

        this.deadline = this.context.getDeadline();
        this.limit = this.context.getLimit();
        this.offset = this.context.getOffset();
        if (Objects.nonNull(wrapped) && Objects.nonNull(offset) && offset > 0) wrapped.skip(offset); // quick skip
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.isNull(wrapped)) {
            var prodAndCons = getProducedConsumed(input, op);
            if (!prodAndCons.getRight().isEmpty()) {
                throw new BackjumpException(prodAndCons.getRight());
            }
            return false;
        }
        if (Objects.nonNull(limit) && produced >= limit) return false;

        if (wrapped.hasNext()) { // actually iterates over the dataset
            if (!context.paused.isPaused() && stopping.apply(context)) { // unless we must stop
                // execution stops immediately, caught by {@link PassageRoot}
                throw new PauseException(op);
            }

            // but if not pause, we create the new binding
            produced += 1;
            wrapped.next();
            BackendBindings<ID, VALUE> newBinding = new BackendBindings<>();

            if (Objects.nonNull(vars.get(SPOC.SUBJECT))) { // ugly x4
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

            try {
                action.accept(newBinding.setParent(input));
            } catch (BackjumpException bje) {
                var prodAncCons = getProducedConsumed(input, op);
                if (prodAncCons.getLeft().stream().noneMatch(bje.problematicVariables::contains)) {
                    throw bje; // forward the exception
                }
                // return false;
            }
            return true;
        }

        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        long remaining = estimateSize();
        if (remaining < 2) { return null; } // no split possible
        // #1 limit the size of this split iterator
        long start = Objects.isNull(offset) ? 0 : offset;
        long splitIndex = start + produced + remaining / 2;
        // #2 create another split iterator of removed size
        PassageExecutionContext<ID,VALUE> newContext =
                new PassageExecutionContextBuilder<ID,VALUE>()
                        .setContext(context).build()
                        .setOffset(splitIndex);
        if (Objects.nonNull(this.limit)) {
            newContext.setLimit(this.limit-splitIndex);
        }
        this.limit = splitIndex - start;

        return new PassageSplitScan<>(newContext, input, op);
    }

    @Override
    public long estimateSize() {
        if (Objects.isNull(wrapped)) return 0;
        long start = Objects.isNull(offset) ? 0 : offset;
        long cardinality = (long) wrapped.cardinality() - start;
        long upperLimit = (Objects.isNull(limit)) ? cardinality : Math.min(cardinality, limit);
        return upperLimit - produced;
    }

    @Override
    public int characteristics() {
        return SUBSIZED | SIZED | NONNULL;
    }

    /* *********************************** UTILS ************************************ */

    private static <ID,VALUE,OP> Pair<Set<Var>, Set<Var>> getProducedConsumed (BackendBindings<ID,VALUE> input, OP op) {
        Set<Var> producedVars = switch (op) {
            case OpTriple triple -> {
                Set<Var> _producedVars = VarUtils.getVars(triple.getTriple());
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
        Set<Var> consumedVars = switch (op) {
            case OpTriple triple -> {
                Set<Var> _consumedVars = VarUtils.getVars(triple.getTriple());
                _consumedVars.removeAll(producedVars);
                yield _consumedVars;
            }
            default -> Set.of();
        };
        return new ImmutablePair<>(producedVars, consumedVars);
    }

}
