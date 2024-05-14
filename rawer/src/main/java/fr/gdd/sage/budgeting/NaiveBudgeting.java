package fr.gdd.sage.budgeting;

import fr.gdd.sage.generics.BackendBindings;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

/**
 * An easy budgeting function that gives all to the first caller.
 * It's meant to not overthink about nested aggregators, group by, etc.
 * at first.
 */
public class NaiveBudgeting implements IBudgeting<Pair<Long, Long>> {

    final Long initialTimeout;
    final Long initialScans;
    long currentTimeout;
    long currentScans;

    public NaiveBudgeting(Long timeout, Long scans) {
        this.initialTimeout = timeout;
        this.initialScans = scans;
        this.currentTimeout = timeout;
        this.currentScans = scans;
    }

    @Override
    public <ID, VALUE> Pair<Long, Long> get(BackendBindings<ID, VALUE> input, Op op, ExecutionContext context) {
        currentScans = 0;
        currentTimeout = 0;
        return new ImmutablePair<>(initialTimeout, initialScans);
    }
}
