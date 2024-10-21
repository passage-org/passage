package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.optimizers.PassageOptimizer;
import fr.gdd.passage.volcano.pause.PassageSavedState;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

/**
 * Quick access to mandatory things of passage to execute properly.
 */
public class PassageExecutionContext<ID,VALUE> extends ExecutionContext {

    public BackendOpExecutor<ID,VALUE> executor;
    public final Backend<ID,VALUE,Long> backend;
    public final BackendCache<ID,VALUE> cache;
    public final PassageOptimizer<ID,VALUE> optimizer;
    public BackendSaver<ID,VALUE,Long> saver;
    public final PassageSavedState savedState;
    public Op query;

    public PassageExecutionContext(ExecutionContext context) {
        super(context);
        this.backend = context.getContext().get(BackendConstants.BACKEND);
        context.getContext().setIfUndef(PassageConstants.SCANS, 0L);
        context.getContext().setIfUndef(BackendConstants.CACHE, new BackendCache<>(backend));
        this.cache = context.getContext().get(BackendConstants.CACHE);

        this.getContext().set(PassageConstants.LOADER, new PassageOptimizer<>(backend, cache));
        optimizer = this.getContext().get(PassageConstants.LOADER);
        if (context.getContext().isTrue(PassageConstants.FORCE_ORDER)) {
            optimizer.forceOrder(); // TODO do this better
        }
        this.getContext().setFalse(PassageConstants.PAUSED);
        this.getContext().set(PassageConstants.PAUSED_STATE, new PassageSavedState());
        this.savedState = this.getContext().get(PassageConstants.PAUSED_STATE);
    }

    /**
     * @param limit The maximum number of results for the query usually set with
     *  a LIMIT clause in the query.
     * @return itself.
     */
    public PassageExecutionContext<ID,VALUE> setLimit(Long limit) {
        this.getContext().set(PassageConstants.LIMIT, limit);
        return this;
    }

    public PassageExecutionContext<ID,VALUE> setOffset(Long offset) {
        this.getContext().set(PassageConstants.OFFSET, offset);
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

    public PassageExecutionContext<ID,VALUE> clone() {
        // TODO create new instances of things that cannot be copied.
        ExecutionContext context =  new ExecutionContext(this.getContext().copy(), this.getActiveGraph(),
                this.getDataset(), this.getExecutor());
        return new PassageExecutionContext<>(context);
    }
}
