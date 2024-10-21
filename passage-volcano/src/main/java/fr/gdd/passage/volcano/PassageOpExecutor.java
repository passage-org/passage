package fr.gdd.passage.volcano;

import fr.gdd.passage.commons.factories.BackendJoinFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.iterators.BackendBind;
import fr.gdd.passage.commons.iterators.BackendFilter;
import fr.gdd.passage.commons.iterators.BackendProject;
import fr.gdd.passage.volcano.iterators.*;
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

    public String pauseAsString() { // null if done.
        Op paused = pause();
        String savedString = Objects.isNull(paused) ? null : OpAsQuery.asQuery(paused).toString();
        context.savedState.setState(savedString); // to export it
        return savedString;
    }

    public Op pause() {
        context.getContext().setTrue(PassageConstants.PAUSED);
        return context.saver.save();
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        root = context.optimizer.optimize(root);
        context.setQuery(root); // mandatory to be saved later on
        // Only need a root that will catch the timeout exception to state that
        // the iterator does not have next.
        return new PassageRoot<>(context, super.execute(root)); // super  must be called because it sets Executor in context.
    }
}
