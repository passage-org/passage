package fr.gdd.raw.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.generics.BindingWithProba;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.accumulators.WanderJoin;
import fr.gdd.raw.executor.RawConstants;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.graph.Node;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.ArrayList;
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

        Backend backend = this.context.getContext().get(RawConstants.BACKEND);
        BackendCache<ID, VALUE> cache = this.context.getContext().get(RawConstants.CACHE);

        produced = null;
        WanderJoin wj = new WanderJoin<>(context.getContext().get(RawConstants.SAVER));
        try{
            Double proba = (Double) wj.visit(((BackendSaver) context.getContext().get(RawConstants.SAVER)).getRoot());
            RawConstants.saveScanProbabilities(context, proba);
            BackendBindings<ID,VALUE> mappings = new BackendBindings<>();

            toReturn.put(RawConstants.MAPPING_PROBABILITY, new BackendBindings.IdValueBackend<ID,VALUE>().setString(proba.toString()));

        }catch (Exception e){
            // TODO : to remove eventually, useful for debugging while still in the works
            // System.out.println("Can't compute probability of retrieving the binding (with wander join) on " + e.getMessage());
        }
        return toReturn;
    }
}