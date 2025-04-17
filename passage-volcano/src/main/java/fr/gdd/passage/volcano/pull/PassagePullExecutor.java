package fr.gdd.passage.volcano.pull;

import fr.gdd.passage.commons.factories.BackendNestedLoopJoinFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.engines.BackendPullExecutor;
import fr.gdd.passage.commons.iterators.BackendBind;
import fr.gdd.passage.commons.iterators.BackendFilter;
import fr.gdd.passage.commons.iterators.BackendProject;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutor;
import fr.gdd.passage.volcano.pull.iterators.*;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Execute only operators that can be continued. Operators work
 * on identifiers by default instead of values, for the sake of performance.
 * That's why it does not extend `OpExecutor` since the latter
 * works on `QueryIterator` that returns `Binding` that provides `Node`.
 */
@Deprecated
public class PassagePullExecutor<ID,VALUE> extends BackendPullExecutor<ID,VALUE> implements PassageExecutor<ID,VALUE> {

    public final PassageExecutionContext<ID,VALUE> context;

    public PassagePullExecutor(PassageExecutionContext<ID,VALUE> context) {
        super(context, BackendProject.factory(), PassageScan.triplesFactory(),
                PassageScan.quadsFactory(), new BackendNestedLoopJoinFactory<>(), PassageUnion.factory(), PassageValues.factory(),
                BackendBind.factory(), BackendFilter.factory(), PassageDistinct.factory(),
                new PassageLimitOffsetFactory<>(), PassageOptional.factory(), PassageCount.factory(),
                PassageService.factory());
        this.context = context;
    }

    public String pauseAsString() { // null if done.
        this.pause(); // result in context.paused already
        return context.paused.getPausedQueryAsString();
    }

    public Op pause() {
        context.paused.pause();
        context.paused.setPausedQuery(context.saver.save());
        return context.paused.getPausedQuery();
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        root = context.optimizer.optimize(root);
        root = new DefaultGraphUriQueryModifier(context).visit(root);
        context.setQuery(root); // mandatory to be saved later on
        // Only need a root that will catch the timeout exception to state that
        // the iterator does not have next.
        return new PassageRoot<>(context, super.execute(root)); // super  must be called because it sets Executor in context.
    }

    // @Override // came from PassagePushExecutor
    public Op execute(String query, Consumer<BackendBindings<ID, VALUE>> consumer) {
        return execute(Algebra.compile(QueryFactory.create(query)), consumer);
    }

    // @Override // came from PassagePushExecutor
    public Op execute(Op query, Consumer<BackendBindings<ID, VALUE>> consumer) {
        execute(query).forEachRemaining(consumer);
        return pause();
    }
}
