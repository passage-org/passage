package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendBindingsFactory;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.federation.LocalServices;
import fr.gdd.passage.volcano.optimizers.PassageOptimizer;
import fr.gdd.passage.volcano.pull.Pause2Next;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Quick access to mandatory things of passage to execute properly.
 */
public class PassageExecutionContext<ID,VALUE> extends ExecutionContext {

    public final Backend<ID,VALUE> backend;
    public BackendCache<ID,VALUE> cache;
    public final PassageOptimizer<ID,VALUE> optimizer;

    @Deprecated // should be removed with the pull iterator model
    public BackendSaver<ID,VALUE,Long> saver;


    public final PassagePaused paused;
    public Op query;
    Long limit; // null is no limit
    Long offset; // null is no offset
    final Long deadline;

    public final long splitScans;
    public final Long maxScans;
    public final AtomicLong scans;
    public final Long maxResults;
    public final AtomicLong results;
    public final Integer maxParallelism;

    @Deprecated // should be removed when configurable engine factory
    public final Boolean backjump;
    public final PassageExecutor<ID,VALUE> executor;
    public final Function<ExecutionContext, PassageExecutor<ID,VALUE>> executorFactory;
    public final BackendBindingsFactory<ID,VALUE> bindingsFactory;
    public final LocalServices localServices;
    public final List<Predicate<PassageExecutionContext<ID,VALUE>>> stoppingConditions;

    public PassageExecutionContext(ExecutionContext context) {
        super(context);
        this.executorFactory = context.getContext().get(BackendConstants.EXECUTOR_FACTORY);
        this.stoppingConditions = context.getContext().get(PassageConstants.STOPPING_CONDITIONS);

        this.backjump = context.getContext().get(PassageConstants.BACKJUMP);
        this.maxScans = context.getContext().get(PassageConstants.MAX_SCANS);
        this.backend = context.getContext().get(BackendConstants.BACKEND);
        this.bindingsFactory = new BackendBindingsFactory<>(backend);
        this.maxParallelism = context.getContext().get(PassageConstants.MAX_PARALLELISM);
        this.localServices = context.getContext().get(PassageConstants.SERVICES);

        context.getContext().setIfUndef(PassageConstants.SCANS, new AtomicLong());
        this.scans = context.getContext().get(PassageConstants.SCANS);
        context.getContext().setIfUndef(PassageConstants.SERVICE_CALLS, new AtomicLong(0L));
        context.getContext().setIfUndef(BackendConstants.CACHE, new BackendCache<>(backend));
        this.cache = context.getContext().get(BackendConstants.CACHE);

        this.getContext().setIfUndef(PassageConstants.LOADER, new PassageOptimizer<>(backend, cache));
        optimizer = this.getContext().get(PassageConstants.LOADER);
        if (context.getContext().isTrue(PassageConstants.FORCE_ORDER)) {
            optimizer.forceOrder(); // TODO do this better
        }
        this.getContext().setIfUndef(PassageConstants.PAUSED, new PassagePaused());
        this.paused = this.getContext().get(PassageConstants.PAUSED);

        this.deadline = this.getContext().get(PassageConstants.DEADLINE);
        this.splitScans = this.getContext().get(PassageConstants.SPLIT_SCANS);

        this.maxResults = this.getContext().get(PassageConstants.MAX_RESULTS);
        context.getContext().setIfUndef(PassageConstants.RESULTS, new AtomicLong());
        this.results = context.getContext().get(PassageConstants.RESULTS);

        this.executor = executorFactory.apply(this); // last just in case
    }

    /**
     * @param limit The maximum number of results for the query usually set with
     *  a LIMIT clause in the query.
     * @return itself.
     */
    public PassageExecutionContext<ID,VALUE> setLimit(Long limit) {
        this.limit = Objects.isNull(limit) || limit < 0 ? null : limit;
        this.getContext().set(PassageConstants.LIMIT, this.limit);
        return this;
    }

    public PassageExecutionContext<ID,VALUE> setOffset(Long offset) {
        this.offset = Objects.isNull(offset) || offset <= 0 ? null : offset;
        this.getContext().set(PassageConstants.OFFSET, this.offset);
        return this;
    }

    public PassageExecutionContext<ID,VALUE> setQuery(Op query) {
        this.query = query;
        // The saver is shared globally, i.e., subOpExecutor will use the same as their
        // parent's
        getContext().setIfUndef(PassageConstants.SAVER, new Pause2Next<>(query, this));
        this.saver = getContext().get(PassageConstants.SAVER);
        return this;
    }

    public PassageExecutionContext<ID,VALUE> setCache(BackendCache<ID,VALUE> cache) {
        if (Objects.nonNull(cache)) {
            getContext().set(BackendConstants.CACHE, cache);
            this.cache = cache;
        }
        return this;
    }

    public Long getLimit() {
        return limit;
    }

    public Long getOffset() {
        return offset;
    }

    public Long getDeadline() {
        return deadline;
    }

    public PassageExecutionContext<ID,VALUE> clone() {
        // TODO create new instances of things that cannot be copied.
        ExecutionContext context =  new ExecutionContext(this.getContext().copy(), this.getActiveGraph(),
                this.getDataset(), this.getExecutor());
        return new PassageExecutionContext<>(context);
    }

    public void incrementNbScans() {
        if (maxScans != Long.MAX_VALUE) { scans.getAndIncrement(); }
    }

    public void incrementNbResults() {
        if (maxResults != Long.MAX_VALUE) { results.getAndIncrement(); }
    }
}
