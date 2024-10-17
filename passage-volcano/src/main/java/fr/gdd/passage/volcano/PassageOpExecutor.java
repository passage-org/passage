package fr.gdd.passage.volcano;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.factories.BackendJoinFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.iterators.BackendBind;
import fr.gdd.passage.commons.iterators.BackendFilter;
import fr.gdd.passage.commons.iterators.BackendProject;
import fr.gdd.passage.volcano.iterators.*;
import fr.gdd.passage.volcano.optimizers.PassageOptimizer;
import fr.gdd.passage.volcano.pause.PassageSavedState;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;

import java.util.Iterator;
import java.util.Objects;

/**
 * Execute only operators that can be continued. Operators work
 * on identifiers by default instead of values, for the sake of performance.
 * That's why it does not extend `OpExecutor` since the latter
 * works on `QueryIterator` that returns `Binding` that provides `Node`.
 */
public class PassageOpExecutor<ID,VALUE> extends BackendOpExecutor<ID,VALUE> {

    public final PassageExecutionContext<ID,VALUE> context;

    public PassageOpExecutor(PassageExecutionContext<ID,VALUE> context) {
        super(context, BackendProject.factory(), PassageScanFactory.factory(),
                new BackendJoinFactory<>(), PassageUnion.factory(), PassageValues.factory(),
                BackendBind.factory(), BackendFilter.factory(), PassageDistinct.factory(),
                new PassageLimitOffset<>(), PassageOptional.factory());
        this.context = context;
    }

    public PassageOpExecutor<ID, VALUE> setTimeout(Long timeout) {
        context.setTimeout(timeout);
        return this;
    }

    public PassageOpExecutor<ID, VALUE> setLimit(Long limit) {
        context.setLimit(limit);
        return this;
    }

    public PassageOpExecutor<ID, VALUE> setBackend(Backend<ID,VALUE,Long> backend) {
        context.setBackend(backend);
        return this;
    }

    public PassageOpExecutor<ID,VALUE> forceOrder() { // TODO do this through an optimizer provider
        context.forceOrder();
        return this;
    }

    public String pauseAsString() {
        Op paused = pause();
        String savedString = Objects.isNull(paused) ? null : OpAsQuery.asQuery(paused).toString();

        PassageSavedState sss = context.getContext().get(PassageConstants.PAUSED_STATE);
        sss.setState(savedString);
        return savedString;
    }

    public Op pause() {
        context.getContext().setTrue(PassageConstants.PAUSED);
        Pause2Next<ID, VALUE> saver = context.getContext().get(PassageConstants.SAVER);
        Op savedOp = saver.save();
        // execCxt.getContext().set(SagerConstants.PAUSED_STATE, savedOp);
        return savedOp;
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        PassageOptimizer<ID,VALUE> optimizer = context.getContext().get(PassageConstants.LOADER);
        root = optimizer.optimize(root);
        context.getContext().set(PassageConstants.SAVER, new Pause2Next<ID,VALUE>(root, context));
        // Only need a root that will catch the timeout exception to state that
        // the iterator does not have next.
        return new PassageRoot<>(context,
                ReturningArgsOpVisitorRouter.visit(this, root, Iter.of(new BackendBindings<>())));
    }
}
