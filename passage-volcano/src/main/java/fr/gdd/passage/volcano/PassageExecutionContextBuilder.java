package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.exceptions.InvalidContexException;
import fr.gdd.passage.volcano.federation.ILocalService;
import fr.gdd.passage.volcano.federation.LocalServices;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;
import java.util.function.Function;

/**
 * A builder for a well-formed execution context for Passage.
 */
public class PassageExecutionContextBuilder<ID,VALUE> {

    private String name; // optional name to make it simpler
    private Backend<ID,VALUE> backend;
    private ExecutionContext context;

    private Long timeout = Long.MAX_VALUE;
    private Long maxScans = Long.MAX_VALUE;
    private Integer maxParallel = 1; // not parallel by default
    private Boolean forceOrder = false;
    private Boolean backjump = false;
    private Long splitScans = 2L;
    private LocalServices localServices = new LocalServices();

    private Function<ExecutionContext, PassageExecutor> executorFactory;
    private Function<PassageExecutionContext<ID,VALUE>, Boolean> stoppingCondition = (ec) ->
            System.currentTimeMillis() >= ec.getDeadline() || ec.scans.get() >= ec.maxScans;

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

        ec.getContext().setIfUndef(PassageConstants.SPLIT_SCANS, splitScans);
        ec.getContext().setIfUndef(PassageConstants.MAX_SCANS, maxScans);
        ec.getContext().setIfUndef(PassageConstants.FORCE_ORDER, forceOrder);
        ec.getContext().setIfUndef(PassageConstants.MAX_PARALLELISM, maxParallel);
        ec.getContext().setIfUndef(PassageConstants.BACKJUMP, backjump);
        ec.getContext().setIfUndef(PassageConstants.STOPPING_CONDITION, stoppingCondition);

        if (maxScans != Long.MAX_VALUE && timeout != Long.MAX_VALUE) {
            this.stoppingCondition = (context) ->
                    System.currentTimeMillis() >= context.getDeadline() || context.scans.get() >= context.maxScans;
        } else if (maxScans != Long.MAX_VALUE) {
            this.stoppingCondition = (context) -> context.scans.get() >= context.maxScans;
        } else if (timeout != Long.MAX_VALUE) {
            this.stoppingCondition = (context) -> System.currentTimeMillis() >= context.getDeadline();
        } else {
            this.stoppingCondition = (context) -> false;
        }


        return new PassageExecutionContext<>(ec);
    }

    public PassageExecutionContextBuilder<ID,VALUE> setBackend(Backend backend) {
        this.backend = backend;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setContext(ExecutionContext context) {
        if (Objects.nonNull(context)) {
            this.context = context;
        }
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> forceOrder() {
        this.forceOrder = true;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> forceOrder(boolean forceOrder) {
        this.forceOrder = forceOrder;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> backjump() {
        this.backjump = true;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setMaxScans(Long maxScans) {
        if (Objects.nonNull(maxScans)) {
            this.maxScans = maxScans;
        }
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setMaxParallel(int maxParallel) {
        if (maxParallel < 1) {
            throw new InvalidContexException("Max parallel must be a positive integer.");
        }
        this.maxParallel = maxParallel;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setSplitScans(long splitScans) {
        if (splitScans <= 1) {
            throw new InvalidContexException("Split scans must be a positive integer.");
        }
        this.splitScans = splitScans;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setExecutorFactory(Function<ExecutionContext, PassageExecutor> executorFactory) {
        this.executorFactory = executorFactory;
        return this;
    }

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
