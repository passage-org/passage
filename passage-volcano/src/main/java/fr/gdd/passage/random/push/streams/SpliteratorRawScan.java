package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.streams.SpliteratorScan;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;

import java.util.Arrays;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Basic scan iterator but provide random values from the triple/quad pattern.
 * Note:
 *     - The backend must support this;
 *     - The values might not be picked uniformly at random.
 */
public class SpliteratorRawScan<ID,VALUE> extends SpliteratorScan<ID,VALUE> {

    public SpliteratorRawScan(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpTriple opTriple) {
        super(context, input, opTriple);

        // The limit MUST be set or random scans provide infinitely values.
        if (Objects.nonNull(this.wrapped)) {
            this.limit = this.context.getLimit(); // if null, stays null
            this.limit = Objects.isNull(limit) ? (long) Math.log(this.wrapped.cardinality() + 2) : limit; // +2 so limit ≥ 1
        }
    }

    public SpliteratorRawScan(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpQuad opQuad) {
        super(context, input, opQuad);

        // The limit MUST be set or random scans provide infinitely values.
        if (Objects.nonNull(this.wrapped)) {
            this.limit = this.context.getLimit(); // if null, stays null
            this.limit = Objects.isNull(limit) ? (long) Math.log(this.wrapped.cardinality() + 2) : limit; // +2 so limit ≥ 1
        }
    }

    /* ********************************** SPLITERATOR ************************************* */

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.isNull(wrapped)) {
            context.incrementNbScans(); // even failures are counted
            return false;
        }
        if (Objects.nonNull(limit) && limit == 0) { return false; } // we produced all

        // offset never moves.
        limit -= 1; // forced limit decreases
        context.incrementNbScans(); // success are counted globally

        // use the underlying iterator to get a new random value
        double proba = wrapped.random(); // TODO what to do about the proba?…
        wrapped.hasNext();
        wrapped.next();
        BackendBindings<ID, VALUE> newBinding = context.bindingsFactory.get();
        Arrays.stream(SPOC.spoc).forEach(code -> registerMapping(newBinding, code));
        newBinding.put(Var.alloc("_probability"), new BackendBindings.IdValueBackend<ID,VALUE>().setString(String.valueOf(proba)));
        action.accept(newBinding.setParent(input));
        return true;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        // TODO simple clone with dividing limit
        //      or bucketing strategy where OFFSETs are changed
        return null; //  for now, we don't authorize splitting
    }

    @Override
    public long estimateSize() {
        if (Objects.isNull(wrapped)) { return 0; } // nothing
        return limit; // produce only `limit` number of elements
    }

    /* ******************************** PAUSE ********************************** */

    @Override
    public double estimateCardinality() {
        return estimateCost();
    }

    @Override
    public int characteristics() {
        return NONNULL | IMMUTABLE;
    }

    @Override
    public double estimateCost() {
        // the same as estimated size since it would require enumerating all.
        if (Objects.isNull(wrapped)) { return 0; }
        return Math.min((long) (wrapped.cardinality() - offset), limit);
    }

    @Override
    public Op pause() {
        return op; // nothing changes…
    }

}