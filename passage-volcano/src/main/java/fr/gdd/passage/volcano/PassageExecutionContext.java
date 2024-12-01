package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.optimizers.PassageOptimizer;
import fr.gdd.passage.volcano.pause.PassagePaused;
import fr.gdd.passage.volcano.pause.Pause2Next;
import fr.gdd.passage.volcano.spliterators.Op2Spliterators;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quick access to mandatory things of passage to execute properly.
 */
public class PassageExecutionContext<ID,VALUE> extends ExecutionContext {

    public final Backend<ID,VALUE,Long> backend;
    public BackendCache<ID,VALUE> cache;
    public final PassageOptimizer<ID,VALUE> optimizer;

    @Deprecated
    public BackendSaver<ID,VALUE,Long> saver;

    public final Op2Spliterators<ID,VALUE> op2its;
    public final Integer maxParallelism;

    public final PassagePaused paused;
    public Op query;
    Long limit; // null is no limit
    Long offset; // null is no offset
    final Long deadline;


    public PassageExecutionContext(ExecutionContext context) {
        super(context);
        this.backend = context.getContext().get(BackendConstants.BACKEND);
        context.getContext().setIfUndef(PassageConstants.OP2ITS, new Op2Spliterators<>());
        this.op2its = context.getContext().get(PassageConstants.OP2ITS);
        this.maxParallelism = context.getContext().get(PassageConstants.MAX_PARALLELISM);
        context.getContext().setIfUndef(PassageConstants.SCANS, new AtomicLong(0L));
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
    }

    /**
     * @param maxResults The number of results that will be returned, whatever the query
     *                   and their LIMIT.
     * @return itself for convenience.
     */
    public PassageExecutionContext<ID,VALUE> setMaxResults(Long maxResults) {
        this.getContext().set(PassageConstants.MAX_RESULTS, maxResults);
        return this;
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
}
