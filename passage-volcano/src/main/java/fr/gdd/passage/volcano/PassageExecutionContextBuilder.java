package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Objects;

/**
 * A builder for a well-formed execution context for Passage.
 */
public class PassageExecutionContextBuilder<ID,VALUE> {

    private Backend<ID,VALUE,?> backend;
    private ExecutionContext context;

    private Long timeout = Long.MAX_VALUE;
    private Long maxScans = Long.MAX_VALUE;
    private Integer maxParallel = 1; // not parallel by default
    private Boolean forceOrder = false;
    private Boolean backjump = false;

    public PassageExecutionContext<ID,VALUE> build() {
        ExecutionContext ec = Objects.isNull(context) ?
                new ExecutionContext(DatasetFactory.empty().asDatasetGraph()):
                context;

        if (ec.getContext().isUndef(BackendConstants.BACKEND) && Objects.isNull(backend)) {
            throw new RuntimeException("Backend undefined.");
        }

        // prioritize execution context's backend
        ec.getContext().setIfUndef(BackendConstants.BACKEND, backend);

        if (ec.getContext().isUndef(PassageConstants.DEADLINE)) {
            ec.getContext().set(PassageConstants.TIMEOUT, timeout);
            long deadline = (System.currentTimeMillis() + timeout <= 0) ? Long.MAX_VALUE : System.currentTimeMillis() + timeout;
            ec.getContext().set(PassageConstants.DEADLINE, deadline);
        }

        ec.getContext().setIfUndef(PassageConstants.MAX_SCANS, maxScans);

        ec.getContext().setIfUndef(PassageConstants.FORCE_ORDER, forceOrder);

        ec.getContext().setIfUndef(PassageConstants.MAX_PARALLELISM, maxParallel);
        ec.getContext().setIfUndef(PassageConstants.BACKJUMP, backjump);

        return new PassageExecutionContext<>(ec);
    }

    public PassageExecutionContextBuilder<ID,VALUE> setBackend(Backend<ID, VALUE,?> backend) {
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

    public PassageExecutionContextBuilder<ID,VALUE> setTimeout(Long timeout) {
        if (Objects.nonNull(timeout)) {
            this.timeout = timeout;
        }
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setMaxParallel(Integer maxParallel) {
        if (Objects.nonNull(maxParallel)) {
            if (maxParallel < 1) {
                throw new RuntimeException("Max parallel must be a positive integer.");
            }
            this.maxParallel = maxParallel;
        }
        return this;
    }

    @Override
    public String toString() {
        return (Objects.nonNull(timeout) && timeout!=Long.MAX_VALUE ? " timeout=" + timeout : "") +
                (Objects.nonNull(maxScans) && maxScans!=Long.MAX_VALUE ? " scans ≤ " + maxScans : "") +
                (forceOrder ? " forceOrder ": "") +
                " parallel ≤ " + maxParallel;
    }
}
