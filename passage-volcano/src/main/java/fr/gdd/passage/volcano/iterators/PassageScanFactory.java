package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.factories.IBackendQuadsFactory;
import fr.gdd.passage.commons.factories.IBackendTriplesFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class PassageScanFactory<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendTriplesFactory<ID,VALUE> tripleFactory() {
        return PassageScanFactory::new;
    }
    public static <ID,VALUE> IBackendQuadsFactory<ID,VALUE> quadFactory() { return PassageScanFactory::new; }

    public static <ID,VALUE> IBackendTriplesFactory<ID,VALUE> factoryTripleLimitOffset() {
        return (context, input, op) -> {
            long offset = context.getContext().get(PassageConstants.OFFSET);
            return new PassageScanFactory<>(context, input, op, offset);
        };
    }

    public static <ID,VALUE> IBackendQuadsFactory<ID,VALUE> factoryQuadLimitOffset() { // same as triple but returns different
        return (context, input, op) -> {
            long offset = context.getContext().get(PassageConstants.OFFSET);
            return new PassageScanFactory<>(context, input, op, offset);
        };
    }

    /* ******************************** ACTUAL ITERATOR FACTORY *********************************** */

    final Long skip; // by offset
    final Backend<ID, VALUE, Long> backend;
    final ExecutionContext context;
    final Op0 tripleOrQuad; // Op0 because can be either a triple pattern, or a quad pattern.
    final BackendCache<ID,VALUE> cache;

    final Iterator<BackendBindings<ID, VALUE>> input;
    BackendBindings<ID, VALUE> inputBinding;
    long produced = 0L;
    final Long limit;

    Iterator<BackendBindings<ID, VALUE>> instantiated = Iter.empty();

    public PassageScanFactory(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, Op0 tripleOrQuad) {
        this.input = input;
        this.tripleOrQuad = tripleOrQuad;
        backend = context.getContext().get(BackendConstants.BACKEND);
        this.context = context;
        this.skip = 0L;
        Pause2Next<ID, VALUE> saver = context.getContext().get(PassageConstants.SAVER);
        saver.register(tripleOrQuad, this);
        this.cache = context.getContext().get(BackendConstants.CACHE);
        this.limit = context.getContext().get(PassageConstants.LIMIT);
    }

    public PassageScanFactory(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, Op0 tripleOrQuad, Long skip) {
        this.input = input;
        this.tripleOrQuad = tripleOrQuad;
        backend = context.getContext().get(BackendConstants.BACKEND);
        this.context = context;
        this.skip = skip;
        Pause2Next<ID, VALUE> saver = context.getContext().get(PassageConstants.SAVER);
        saver.register(tripleOrQuad, this);
        this.cache = context.getContext().get(BackendConstants.CACHE);
        this.limit = context.getContext().get(PassageConstants.LIMIT);
    }

    @Override
    public boolean hasNext() {
        if (Objects.nonNull(limit) && limit != Long.MIN_VALUE && produced >= limit) {return false;} // when limit exists
        if (!instantiated.hasNext() && !input.hasNext()) {
            return false;
        } else while (!instantiated.hasNext() && input.hasNext()) {
            inputBinding = input.next();

            try{
                instantiated = switch (tripleOrQuad) {
                    case OpTriple opTriple -> {
                        Tuple<ID> spo = Substitutor.substitute(opTriple.getTriple(), inputBinding, cache);
                        yield new PassageScan<>(context, tripleOrQuad, spo, backend.search(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT)));
                    }
                    case OpQuad opQuad -> {
                        Tuple<ID> spo =Substitutor.substitute(opQuad.getQuad(), inputBinding, cache);
                        yield new PassageScan<>(context, tripleOrQuad, spo,
                                backend.search(spo.get(SPOC.SUBJECT), spo.get(SPOC.PREDICATE), spo.get(SPOC.OBJECT), spo.get(SPOC.GRAPH)));
                    }
                    default -> throw new UnsupportedOperationException("Operator not handle here:" + tripleOrQuad);
                };

                if (Objects.nonNull(skip) && skip > 0L) {
                    ((PassageScan<ID,VALUE>) instantiated).skip(skip);
                }
            } catch (NotFoundException | IllegalArgumentException e){
                instantiated = Iter.empty(); // can be an issue if we cast later on
            }
        }

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        produced += 1;
        return instantiated.next().setParent(inputBinding);
    }


    public double cardinality() {
        if (instantiated instanceof PassageScan<ID,VALUE> scan) {
            return scan.cardinality();
        }
        return 0.;
    }

    public long offset() {
        if (instantiated instanceof PassageScan<ID,VALUE> scan) {
            return scan.current();
        }
        return 0L;
    }

    /**
     * @return The Jena operator that summarizes the current state of this scan iterator.
     * It is made of `Bind … As …` to save the state that created this iterator, plus the triple pattern
     * itself unmoved, plus a slice operator that defines an offset.
     * It returns `null` when the wrapped scan iterator does not have a next binding.
     */
    public Op pause() {
        if (!instantiated.hasNext()) {
            return null;
        }

        Set<Var> vars = inputBinding.variables();
        OpSequence seq = OpSequence.create();
        for (Var v : vars) {
            seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(inputBinding.getBinding(v).getString())));
        }
        seq.add(tripleOrQuad);

        Op seqOrSingle = seq.size() > 1 ? seq : seq.get(0);
        long newLimit = Objects.isNull(limit) || limit == Long.MIN_VALUE ? Long.MIN_VALUE : limit - produced;
        return new OpSlice(seqOrSingle, ((PassageScan<ID, VALUE>) instantiated).current(), newLimit);
    }

}
