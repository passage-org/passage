package fr.gdd.raw.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.raw.executor.RawConstants;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

/**
 * Iterator in charge of checking some stopping conditions, e.g., the execution
 * time reached the timeout.
 */
public class RandomRoot<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Long limit;
    final Long deadline;
    final Op op;
    final RawOpExecutor<ID, VALUE> executor;
    final ExecutionContext context;

    Long count = 0L;
    Iterator<BackendBindings<ID, VALUE>> current;
    BackendBindings<ID, VALUE> produced;

    public RandomRoot(RawOpExecutor<ID, VALUE> executor, ExecutionContext context, Op op) {
        this.limit = context.getContext().get(RawConstants.LIMIT, Long.MAX_VALUE);
        this.deadline = context.getContext().get(RawConstants.DEADLINE, Long.MAX_VALUE);
        this.executor = executor;
        this.op = op;
        this.context = context;
    }

    @Override
    public boolean hasNext() {
        while (Objects.isNull(produced)) {
            if (shouldStop()) {
                return false;
            }
            // TODO input as Iterator<BindingId2Value>
            if (Objects.nonNull(current) && current.hasNext()) {
                produced = current.next();
            } else {
                current = ReturningArgsOpVisitorRouter.visit(this.executor, this.op, Iter.of(new BackendBindings<>()));
            }
        }
        return true;
    }

    private boolean shouldStop() {
        return System.currentTimeMillis() > deadline ||
                count >= limit ||
                context.getContext().getLong(RawConstants.SCANS, 0L) >= limit;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        ++count;
        BackendBindings<ID, VALUE> toReturn = produced; // ugly :(
        produced = null;
        return toReturn;
    }
}
