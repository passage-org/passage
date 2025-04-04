package fr.gdd.raw.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.raw.accumulators.WanderJoin;
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
    final Long attemptLimit;
    final Long deadline;
    final Op op;
    final RawOpExecutor<ID, VALUE> executor;
    final ExecutionContext context;

    Long count = 0L;
    Iterator<BackendBindings<ID, VALUE>> current;
    BackendBindings<ID, VALUE> produced;

    public RandomRoot(RawOpExecutor<ID, VALUE> executor, ExecutionContext context, Op op) {
        this.limit = context.getContext().get(RawConstants.LIMIT, Long.MAX_VALUE);
        this.attemptLimit = context.getContext().get(RawConstants.ATTEMPT_LIMIT, Long.MAX_VALUE);
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

            if(Objects.isNull(current)){
                current = ReturningArgsOpVisitorRouter.visit(this.executor, this.op, Iter.of(new BackendBindings<>()));
                produced = null;
            }

            if(current.hasNext()) {
                produced = current.next();
            } else {
                RawConstants.incrementRandomWalkAttempts(context);
                current = null;
            }


        }
        return true;
    }

    private boolean shouldStop() {
        return System.currentTimeMillis() > deadline ||
                count >= limit ||
                context.getContext().getLong(RawConstants.SCANS, 0L) >= limit ||
                RawConstants.getRandomWalkAttempts(context) >= attemptLimit;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        ++count;
        BackendBindings<ID, VALUE> toReturn = produced; // ugly :(

        produced = null;

        BackendSaver bs = context.getContext().get(RawConstants.SAVER);

        WanderJoin wj = new WanderJoin<>(bs);

        try{
            Double proba = (Double) wj.visit(bs.getRoot());
            RawConstants.saveScanProbabilities(context, proba);

            toReturn.put(RawConstants.MAPPING_PROBABILITY, new BackendBindings.IdValueBackend<ID,VALUE>().setString(proba.toString()));

        }catch (Exception e){
            // nothing to do, we end here when the iterator tree contains an operator for which computing a probability
            // for wander join has not been implemented.
        }
        return toReturn;
    }
}