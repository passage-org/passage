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

    private Backend<ID,VALUE,Long> backend;
    private ExecutionContext context;

    private Long timeout = Long.MAX_VALUE;
    private Long maxScans = Long.MAX_VALUE;
    private Boolean forceOrder = false;

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

        return new PassageExecutionContext<>(ec);
    }

    public PassageExecutionContextBuilder<ID,VALUE> setBackend(Backend<ID, VALUE, Long> backend) {
        this.backend = backend;
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setContext(ExecutionContext context) {
        if (Objects.nonNull(context)) {
            this.context = context;
        }
        return this;
    }

    public PassageExecutionContextBuilder<ID,VALUE> setForceOrder(Boolean forceOrder) {
        if (Objects.nonNull(forceOrder)) {
            this.forceOrder = forceOrder;
        }
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

}
