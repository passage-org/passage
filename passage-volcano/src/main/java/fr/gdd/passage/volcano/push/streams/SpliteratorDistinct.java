package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.exceptions.PauseException;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

import static fr.gdd.passage.volcano.push.Pause2Continuation.DONE;

/**
 * A wrapper that iterates over distinct values.
 */
@Deprecated
public class SpliteratorDistinct<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>>, PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final Backend<ID,VALUE> backend;
    final BackendCache<ID,VALUE> cache;
    final Op0 op;
    final Long id; // The identifier is actually the starting offset
    // Set<Var> producedVars;
    // Set<Var> consumedVars;
    final OpProject distinctVars;

    Long offset;
    Long limit;

    BackendIterator<ID, VALUE> wrapped;
    Tuple<Var> vars; // needed to create bindings var -> value

    public SpliteratorDistinct(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, Op0 tripleOrQuad) {
        this.context = context;
        this.backend = this.context.backend;
        this.cache = this.context.cache;
        this.op = tripleOrQuad;
        this.input = input;
        this.distinctVars = context.getContext().get(PassageConstants.PROJECT);

        try {
            switch (tripleOrQuad) {
                case OpTriple opTriple -> {
                    Tuple<ID> spo = Substitutor.substitute(opTriple.getTriple(), input, cache);

                    // TODO factorize this ugly code
                    Set<Integer> distinctVarsCodes = new HashSet<>();
                    if (opTriple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(SPOC.SUBJECT)) &&
                            (Objects.isNull(distinctVars) || distinctVars.getVars().contains(opTriple.getTriple().getSubject()))) {
                        distinctVarsCodes.add(SPOC.SUBJECT);
                    }
                    if (opTriple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(SPOC.PREDICATE)) &&
                            (Objects.isNull(distinctVars) || distinctVars.getVars().contains(opTriple.getTriple().getPredicate()))) {
                        distinctVarsCodes.add(SPOC.PREDICATE);
                    }
                    if (opTriple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(SPOC.OBJECT)) &&
                            (Objects.isNull(distinctVars) || distinctVars.getVars().contains(opTriple.getTriple().getObject()))) {
                        distinctVarsCodes.add(SPOC.OBJECT);
                    }

                    this.vars = TupleFactory.create3(
                            opTriple.getTriple().getSubject().isVariable() && Objects.isNull(spo.get(SPOC.SUBJECT)) ? Var.alloc(opTriple.getTriple().getSubject()) : null,
                            opTriple.getTriple().getPredicate().isVariable() && Objects.isNull(spo.get(SPOC.PREDICATE)) ? Var.alloc(opTriple.getTriple().getPredicate()) : null,
                            opTriple.getTriple().getObject().isVariable() && Objects.isNull(spo.get(SPOC.OBJECT)) ? Var.alloc(opTriple.getTriple().getObject()) : null);
                    this.wrapped = backend.searchDistinct(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT), distinctVarsCodes);
                }
                case OpQuad opQuad -> {
                    Tuple<ID> spoc = Substitutor.substitute(opQuad.getQuad(), input, cache);

                    // TODO factorize this ugly code
                    Set<Integer> distinctVarsCodes = new HashSet<>();
                    if (opQuad.getQuad().getSubject().isVariable() && Objects.isNull(spoc.get(SPOC.SUBJECT)) &&
                            (Objects.isNull(distinctVars) || distinctVars.getVars().contains(opQuad.getQuad().getSubject()))) {
                        distinctVarsCodes.add(SPOC.SUBJECT);
                    }
                    if (opQuad.getQuad().getPredicate().isVariable() && Objects.isNull(spoc.get(SPOC.PREDICATE)) &&
                            (Objects.isNull(distinctVars) || distinctVars.getVars().contains(opQuad.getQuad().getPredicate()))) {
                        distinctVarsCodes.add(SPOC.PREDICATE);
                    }
                    if (opQuad.getQuad().getObject().isVariable() && Objects.isNull(spoc.get(SPOC.OBJECT)) &&
                            (Objects.isNull(distinctVars) || distinctVars.getVars().contains(opQuad.getQuad().getObject()))) {
                        distinctVarsCodes.add(SPOC.OBJECT);
                    }
                    if (opQuad.getQuad().getGraph().isVariable() && Objects.isNull(spoc.get(SPOC.GRAPH)) &&
                            (Objects.isNull(distinctVars) || distinctVars.getVars().contains(opQuad.getQuad().getGraph()))) {
                        distinctVarsCodes.add(SPOC.GRAPH);
                    }

                    this.vars = TupleFactory.create4(
                            opQuad.getQuad().getSubject().isVariable() && Objects.isNull(spoc.get(SPOC.SUBJECT)) ? Var.alloc(opQuad.getQuad().getSubject()) : null,
                            opQuad.getQuad().getPredicate().isVariable() && Objects.isNull(spoc.get(SPOC.PREDICATE)) ? Var.alloc(opQuad.getQuad().getPredicate()) : null,
                            opQuad.getQuad().getObject().isVariable() && Objects.isNull(spoc.get(SPOC.OBJECT)) ? Var.alloc(opQuad.getQuad().getObject()) : null,
                            opQuad.getQuad().getGraph().isVariable() && Objects.isNull(spoc.get(SPOC.GRAPH)) ? Var.alloc(opQuad.getQuad().getGraph()) : null);
                    this.wrapped = backend.searchDistinct(spoc.get(SPOC.SUBJECT), spoc.get(SPOC.PREDICATE), spoc.get(SPOC.OBJECT), spoc.get(SPOC.GRAPH), distinctVarsCodes);
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

        this.limit = this.context.getLimit(); // if null, stays null
        this.offset = Objects.nonNull(this.context.getOffset()) ? this.context.getOffset() : 0;
        this.id = this.offset;

//        if (this.context.backjump && Objects.isNull(wrapped)) { // no throw, no overhead
//            unregister();
//            getLazyProducedConsumed(input);
//            // throws immediately if there are no results, no need to try advance
//            if (!consumedVars.isEmpty()) {
//                throw new BackjumpException(consumedVars);
//            }
//        }

        if (Objects.nonNull(wrapped) && offset > 0) wrapped.skip(offset); // quick skip
    }


    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.isNull(wrapped)) {
            // unregister();
            return false; }
        if (Objects.nonNull(limit) && limit == 0) {
            //unregister();
            return false; } // we produced all

        if (wrapped.hasNext()) { // actually iterates over the dataset
            if (!context.paused.isPaused() && context.stoppingCondition.apply(context)) { // unless we must stop
                throw new PauseException(op); // execution stops immediately, caught at the root
            }

            // but if not pause, we create the new binding
            offset += 1;
            if (Objects.nonNull(limit)) { limit -= 1 ; }
            wrapped.next();
            if (context.maxScans != Long.MAX_VALUE) { context.scans.getAndIncrement(); } // don't even try if not useful
            BackendBindings<ID, VALUE> newBinding = context.bindingsFactory.get();

            if (Objects.nonNull(vars.get(SPOC.SUBJECT)) && (Objects.isNull(distinctVars) || distinctVars.getVars().contains(vars.get(SPOC.SUBJECT)))) { // ugly x4
                newBinding.put(vars.get(SPOC.SUBJECT), wrapped.getId(SPOC.SUBJECT), context.backend).setCode(vars.get(SPOC.SUBJECT), SPOC.SUBJECT);
            }
            if (Objects.nonNull(vars.get(SPOC.PREDICATE)) && (Objects.isNull(distinctVars) || distinctVars.getVars().contains(vars.get(SPOC.PREDICATE)))) {
                newBinding.put(vars.get(SPOC.PREDICATE), wrapped.getId(SPOC.PREDICATE), context.backend).setCode(vars.get(SPOC.PREDICATE), SPOC.PREDICATE);
            }
            if (Objects.nonNull(vars.get(SPOC.OBJECT)) && (Objects.isNull(distinctVars) || distinctVars.getVars().contains(vars.get(SPOC.OBJECT)))) {
                newBinding.put(vars.get(SPOC.OBJECT), wrapped.getId(SPOC.OBJECT), context.backend).setCode(vars.get(SPOC.OBJECT), SPOC.OBJECT);
            }
            if (vars.len() > 3 && Objects.nonNull(vars.get(SPOC.GRAPH)) && (Objects.isNull(distinctVars) || distinctVars.getVars().contains(vars.get(SPOC.GRAPH)))) {
                newBinding.put(vars.get(SPOC.GRAPH), wrapped.getId(SPOC.GRAPH), context.backend).setCode(vars.get(SPOC.GRAPH), SPOC.GRAPH);
            }

    //          try {
                action.accept(newBinding.setParent(input));
//            } catch (BackjumpException bje) {
//                getLazyProducedConsumed(input);
//                if (producedVars.stream().noneMatch(bje.problematicVariables::contains)) {
//                    unregister();
//                    throw bje; // forward the exception
//                }
//            }
            return true;
        }

        // unregister();
        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        // TODO to split, we need to check that the last produced is not produced by the next iterator.
        //      For example:
        //      [t1 t2 t3 t3 ] [ t3 t4 t5 ] => here t3 should be produced by only the second split even
        //      if t3 is included in the first split
        return null; // does not split yet.
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE; // TODO could be estimated actually, using sampling, but for now nothing
    }

    @Override
    public int characteristics() {
        return 0; // TODO maybe ORDERED
    }

    /* ********************************************************************************** */

    @Override
    public Op pause() {
        if (Objects.nonNull(limit) && limit == 0) return DONE;
        // save the whole context
        Op toSave = input.joinWith(op);
        // update LIMIT and OFFSET
        offset = wrapped.current(); // we rely on the offset of the underlying iterator
        long newLimit = Objects.isNull(limit) ? Long.MIN_VALUE : limit;
        long newOffset = Objects.isNull(offset) || offset == 0 ? Long.MIN_VALUE : offset; // to simplify the query

        OpProject topProjectIfItExists = context.getContext().get(PassageConstants.PROJECT);
        toSave = (Objects.nonNull(topProjectIfItExists)) ?
                new OpDistinct(OpCloningUtil.clone(topProjectIfItExists, toSave)):
                new OpDistinct(toSave);

        toSave = (newLimit == Long.MIN_VALUE && newOffset == Long.MIN_VALUE) ?
                toSave:
                new OpSlice(toSave, newOffset, newLimit); // if either LIMIT or OFFSET, we need to create a subquery

        return toSave;
    }
}
