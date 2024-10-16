package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.CacheId;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.optimizers.PassageOptimizer;
import fr.gdd.passage.volcano.pause.PassageSavedState;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.io.Serializable;

/**
 * Quick access to mandatory things of passage to execute properly.
 */
public class PassageExecutionContext<ID,VALUE> extends ExecutionContext {

    Backend<ID,VALUE,Long> backend;
    CacheId<ID,VALUE> cache;

    public PassageExecutionContext(Backend<ID,VALUE,Long> backend) {
        super(new ExecutionContext(DatasetFactory.empty().asDatasetGraph()));
        this.getContext().setIfUndef(PassageConstants.SCANS, 0L);
        this.getContext().setIfUndef(PassageConstants.LIMIT, Long.MAX_VALUE);
        this.getContext().setIfUndef(PassageConstants.TIMEOUT, Long.MAX_VALUE);
        this.getContext().setFalse(PassageConstants.PAUSED);
        this.getContext().set(PassageConstants.PAUSED_STATE, new PassageSavedState() );
        this.setBackend(backend);
    }

    public PassageExecutionContext<ID,VALUE> setTimeout(Long timeout) {
        this.getContext().set(PassageConstants.TIMEOUT, timeout);
        long deadline = (System.currentTimeMillis() + timeout <= 0) ? Long.MAX_VALUE : System.currentTimeMillis() + timeout;
        this.getContext().set(PassageConstants.DEADLINE, deadline);
        return this;
    }

    public PassageExecutionContext<ID,VALUE> setLimit(Long limit) {
        this.getContext().set(PassageConstants.LIMIT, limit);
        return this;
    }

    public PassageExecutionContext<ID,VALUE> setBackend(Backend<ID,VALUE,Long> backend) {
        this.getContext().set(PassageConstants.BACKEND, backend);
        this.backend = backend;
        this.cache = new CacheId<>(backend);
        this.getContext().setIfUndef(PassageConstants.CACHE, this.cache);
        // as setifundef so outsiders can configure their own list of optimizers
        this.getContext().setIfUndef(PassageConstants.LOADER, new PassageOptimizer<>(backend, cache));
        return this;
    }


    public CacheId<ID, VALUE> getCache() {
        return cache;
    }

    public Backend<ID, VALUE, Long> getBackend() {
        return backend;
    }
}
