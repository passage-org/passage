package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.exceptions.InvalidContexException;
import fr.gdd.passage.volcano.federation.ILocalService;
import fr.gdd.passage.volcano.federation.LocalServices;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A builder for a well-formed execution context for Passage.
 */
public class PassageExecutionContextBuilder<ID,VALUE> {

    private String name; // optional name to make it simpler
    private Backend backend;
    private ExecutionContext context;

    private Long timeout = Long.MAX_VALUE;
    private Long maxResults = Long.MAX_VALUE;
    private Long maxScans = Long.MAX_VALUE;
    private Integer maxParallel = 1; // not parallel by default
    private Long splitScans = 2L;
    private LocalServices localServices = new LocalServices();
    private Boolean forceOrder = false;
    private Boolean backJump = false;

    private Function<ExecutionContext, PassageExecutor> executorFactory;
    private final List<Predicate<PassageExecutionContext<ID,VALUE>>> stoppingConditions = new ArrayList<>();

    /* *************************** BUILDER OPERATIONS **************************** */

    public PassageExecutionContext<ID,VALUE> build() {
        ExecutionContext ec = Objects.isNull(context) ?
                ExecutionContext.create(DatasetFactory.empty().asDatasetGraph()):
                context;

        if (ec.getContext().isUndef(BackendConstants.BACKEND) && Objects.isNull(backend)) {
            throw new InvalidContexException("Backend undefined.");
        } else {
            // prioritize execution context's backend
            ec.getContext().setIfUndef(BackendConstants.BACKEND, backend);
        }

        try {
            ec.getContext().set(PassageConstants.SERVICES, localServices.clone()); // TODO maybe ifUndef?
        } catch (CloneNotSupportedException e) {
            throw new InvalidContexException("Local service did not setup properly.");
        }

        if (ec.getContext().isUndef(BackendConstants.EXECUTOR_FACTORY) && Objects.isNull(executorFactory)) {
            throw new InvalidContexException("Executor factory undefined.");
        } else {
            ec.getContext().setIfUndef(BackendConstants.EXECUTOR_FACTORY, executorFactory);
        }

        if (ec.getContext().isUndef(PassageConstants.DEADLINE)) {
            ec.getContext().set(PassageConstants.TIMEOUT, timeout);
            // handle overflow
            long deadline = (System.currentTimeMillis() + timeout <= 0) ? Long.MAX_VALUE : System.currentTimeMillis() + timeout;
            ec.getContext().set(PassageConstants.DEADLINE, deadline);
        }

        ec.getContext().setIfUndef(PassageConstants.BACKJUMP, backJump);

        ec.getContext().setIfUndef(PassageConstants.MAX_RESULTS, maxResults);
        ec.getContext().setIfUndef(PassageConstants.SPLIT_SCANS, splitScans);
        ec.getContext().setIfUndef(PassageConstants.MAX_SCANS, maxScans);
        ec.getContext().setIfUndef(PassageConstants.FORCE_ORDER, forceOrder);
        ec.getContext().setIfUndef(PassageConstants.MAX_PARALLELISM, maxParallel);

        ec.getContext().setIfUndef(PassageConstants.STOPPING_CONDITIONS, stoppingConditions);
        if (maxScans != Long.MAX_VALUE)
            stoppingConditions.add(c -> c.scans.get() >= c.maxScans);

        if (maxResults != Long.MAX_VALUE)
            stoppingConditions.add(c -> c.results.get() >= c.maxResults);

        if (timeout != Long.MAX_VALUE)
            stoppingConditions.add(c -> System.currentTimeMillis() >= c.getDeadline());

        return new PassageExecutionContext<>(ec);
    }

    /**
     * @param backend The backend containing the graph to query.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setBackend(Backend backend) {
        this.backend = backend;
        return this;
    }

    /**
     * @param context The execution context to preset.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setContext(ExecutionContext context) {
        if (Objects.nonNull(context)) { this.context = context; }
        return this;
    }

    /**
     * Disable the reordering of graph pattern. Query execution performance might be
     * impacted, for the better when the heuristic wrongfully rewrite the query, or converse.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> forceOrder() {
        this.forceOrder = true;
        return this;
    }

    /**
     * @param forceOrder Whether the graph pattern should be reordered or should
     *                   keep the order provided by the requester.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> forceOrder(boolean forceOrder) {
        this.forceOrder = forceOrder;
        return this;
    }

    // TODO create engine configurations including back jump instead.
    public PassageExecutionContextBuilder<ID,VALUE> backjump() {
        this.backJump = true;
        return this;
    }

    /**
     * @param maxScans The maximum number of scans, i.e., number of `hasNext` `next`
     *                 to be performed on the iterators during query execution.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setMaxScans(Long maxScans) {
        if (Objects.nonNull(maxScans)) { this.maxScans = maxScans; }
        return this;
    }

    /**
     * @param maxResults The maximum number of results to fetch before stopping query
     *                   execution. This is useful when the number of results is large,
     *                   and therefore, serializing, keeping them in memory, and sending
     *                   them over the network might be costly.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setMaxResults(Long maxResults) {
        this.maxResults = maxResults;
        return this;
    }

    /**
     * @param timeout The maximum query execution time, i.e., how long before this
     *                query execution gets stopped.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * @param maxParallel Set the number of threads to perform the query execution when
     *                    possible. Not all SPARQL operators allow parallelism.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setMaxParallel(int maxParallel) {
        if (maxParallel < 1) { throw new InvalidContexException("Max parallel must be a positive integer.");}
        this.maxParallel = maxParallel;
        return this;
    }

    /**
     * @param splitScans Among others, scan iterators allow parallel execution. However,
     *                   they require to have a minimal number of elements to iterate over.
     *                   `splitScans` defines this minimum. Its value should be at least 2.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setSplitScans(long splitScans) {
        if (splitScans <= 1) { throw new InvalidContexException("Split scans must be a positive integer greater than 1.");}
        this.splitScans = splitScans;
        return this;
    }

    /**
     * @param executorFactory One of the most important setting. It defines the factory that will build
     *                        an engine based on a configuration. The engine must follow the `PassageExecutor`
     *                        interface.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setExecutorFactory(Function<ExecutionContext, PassageExecutor> executorFactory) {
        this.executorFactory = executorFactory;
        return this;
    }

    /**
     * @param name Convenience for debugging: Adds a text to output configuration.
     * @return this for convenience.
     */
    public PassageExecutionContextBuilder<ID,VALUE> setName(String name) {
        this.name = name;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> registerService (String uri, ILocalService service) {
        localServices.register(uri, service);
        return this;
    }

    @Override
    public String toString() {
        return  ((Objects.nonNull(name)) ? name + " " : "") +
                (Objects.nonNull(timeout) && timeout!=Long.MAX_VALUE ? " timeout=" + timeout : "") +
                (Objects.nonNull(maxScans) && maxScans!=Long.MAX_VALUE ? " scans ≤ " + maxScans : "") +
                (forceOrder ? " forceOrder ": "") +
                (maxParallel > 1 ? " parallel ≤ " + maxParallel : "") ;
    }
}
