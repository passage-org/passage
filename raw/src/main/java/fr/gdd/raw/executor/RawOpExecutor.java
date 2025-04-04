package fr.gdd.raw.executor;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.factories.BackendNestedLoopJoinFactory;
import fr.gdd.passage.commons.factories.IBackendBindsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.iterators.BackendBind;
import fr.gdd.passage.commons.iterators.BackendFilter;
import fr.gdd.passage.commons.iterators.BackendProject;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.volcano.optimizers.CardinalityJoinOrdering;
import fr.gdd.passage.volcano.transforms.BGP2Triples;
import fr.gdd.passage.volcano.transforms.Graph2Quads;
import fr.gdd.passage.volcano.transforms.Triples2BGP;
import fr.gdd.raw.LeftJoinizeNonGroupKeys;
import fr.gdd.raw.accumulators.AccumulatorFactory;
import fr.gdd.raw.budgeting.NaiveBudgeting;
import fr.gdd.raw.iterators.*;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Execute the query and exactly the query that has been asked.
 * If an operator is not implemented, then it returns the explicit mention
 * that it's not implemented. No surprises.
 */
public class RawOpExecutor<ID, VALUE> extends BackendOpExecutor<ID, VALUE> { // output

    final ExecutionContext execCxt;
    Backend<ID, VALUE, ?> backend;
    BackendCache<ID, VALUE> cache;


    public RawOpExecutor(ExecutionContext context) {
        // TODO default execution context to unburden this class

        super(context, BackendProject.factory(), RandomScanFactory.tripleFactory(),
                RandomScanFactory.quadFactory(), new BackendNestedLoopJoinFactory<>(), null, RandomValues.factory(),
                BackendBind.factory(), BackendFilter.factory(), null,
                null, RawOptional.factory(), null);
        this.execCxt = context;
        this.execCxt.getContext().setIfUndef(RawConstants.SCAN_PROBABILITIES, new ArrayList<>());
        this.execCxt.getContext().setIfUndef(RawConstants.RANDOM_WALK_ATTEMPTS, new RawConstants.Wrapper<Long>(0L));

    }

    public RawOpExecutor() {
        // This creates a brandnew execution context, but it's important
        // that `setBackend` is called, or it will throw at runtime.

        super(new ExecutionContext(DatasetFactory.empty().asDatasetGraph()), BackendProject.factory(), RandomScanFactory.tripleFactory(),
                RandomScanFactory.quadFactory(), new BackendNestedLoopJoinFactory<>(), null, RandomValues.factory(),
                BackendBind.factory(), BackendFilter.factory(), null,
                null, RawOptional.factory(), null);

        execCxt = context;


        execCxt.getContext().setIfUndef(RawConstants.SCANS, 0L);
        execCxt.getContext().setIfUndef(RawConstants.SCAN_PROBABILITIES, new ArrayList<>());
        execCxt.getContext().setIfUndef(RawConstants.RANDOM_WALK_ATTEMPTS, new RawConstants.Wrapper<Long>(0L));
        execCxt.getContext().setIfUndef(RawConstants.LIMIT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(RawConstants.TIMEOUT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(RawConstants.BUDGETING, new NaiveBudgeting(
                execCxt.getContext().get(RawConstants.TIMEOUT),
                execCxt.getContext().get(RawConstants.LIMIT)));
    }

    public RawOpExecutor<ID, VALUE> setTimeout(Long timeout) {
        execCxt.getContext().set(RawConstants.TIMEOUT, timeout);
        long deadline = System.currentTimeMillis() + timeout;
        execCxt.getContext().set(RawConstants.DEADLINE, deadline > 0 ? deadline : Long.MAX_VALUE); // handle overflow
        return this;
    }

    public RawOpExecutor<ID, VALUE> setLimit(Long limit) {
        execCxt.getContext().set(RawConstants.LIMIT, limit);
        return this;
    }

    public RawOpExecutor<ID, VALUE> setCache(BackendCache<ID,VALUE> cache) {
        execCxt.getContext().set(RawConstants.CACHE, cache);
        this.cache = cache;
        return this;
    }

    public RawOpExecutor<ID,VALUE> setBackend(Backend<ID,VALUE,?> backend) {
        this.backend = backend;
        execCxt.getContext().set(RawConstants.BACKEND, backend);
        execCxt.getContext().setIfUndef(RawConstants.CACHE, new BackendCache<>(this.backend));
        this.cache = execCxt.getContext().get(RawConstants.CACHE);
        return this;
    }

    public RawOpExecutor<ID,VALUE> setMaxThreads(int maxThreads) {
        execCxt.getContext().set(RawConstants.MAX_THREADS, maxThreads);
        return this;
    }

    public RawOpExecutor<ID,VALUE> forceOrder() {
        execCxt.getContext().setTrue(RawConstants.FORCE_ORDER);
        return this;
    }

    /**
     * Depending on the backend, it might be profitable to use another count-distinct
     * algorithm. By default, it's crawd.
     * @param factory The factory that create the approximate accumulators.
     * @return this, for convenience.
     */
    public RawOpExecutor<ID,VALUE> setCountDistinct(AccumulatorFactory<ID,VALUE> factory) {
        execCxt.getContext().set(RawConstants.COUNT_DISTINCT_FACTORY, factory);
        return this;
    }

    public Backend<ID, VALUE, ?> getBackend() {
        return backend;
    }
    public ExecutionContext getExecutionContext() {
        return execCxt;
    }
    public BackendCache<ID, VALUE> getCache() { return cache;}

    /* ************************************************************************ */

    public Iterator<BackendBindings<ID,VALUE>> execute(String queryAsString) {
        return this.execute(Algebra.compile(QueryFactory.create(queryAsString)));
    }

    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        // #A reordering of bgps if need be
        root = ReturningOpVisitorRouter.visit(new Graph2Quads(), root);
        if (execCxt.getContext().isFalseOrUndef(RawConstants.FORCE_ORDER)) {
            root = ReturningOpVisitorRouter.visit(new Triples2BGP(), root);
            root = new CardinalityJoinOrdering<>(backend, cache).visit(root); // need to have bgp to optimize, no tps

        }

        // If a select query, apply optional to everything, to retrieve incomplete random walks
        Query currentQuery = context.getContext().get(ARQConstants.sysCurrentQuery);
        if(currentQuery.isSelectType()){
            root = ReturningOpVisitorRouter.visit(new LeftJoinizeNonGroupKeys(), root);
        }

        root = ReturningOpVisitorRouter.visit(new BGP2Triples(), root);
        root = new DefaultGraphUriQueryModifier(execCxt).visit(root);
        execCxt.getContext().set(RawConstants.SAVER, new BackendSaver<>(backend, root));
        return new RandomRoot<>(this, execCxt, root);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple triple, Iterator<BackendBindings<ID, VALUE>> input) {
        return new RandomScanFactory<>(execCxt, input, triple);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpQuad quad, Iterator<BackendBindings<ID, VALUE>> input){
        return new RandomScanFactory<>(execCxt, input, quad);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        return new ProjectIterator<>(project, ReturningArgsOpVisitorRouter.visit(this, project.getSubOp(), input));
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpExtend extend, Iterator<BackendBindings<ID, VALUE>> input) {
        IBackendBindsFactory<ID,VALUE> factory = BackendBind.factory();
        return factory.get(getExecutionContext(), input, extend);
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpJoin join, Iterator<BackendBindings<ID, VALUE>> input) {
        input = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), input);
        return ReturningArgsOpVisitorRouter.visit(this, join.getRight(), input);
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTable table, Iterator<BackendBindings<ID, VALUE>> input) {
        if (table.isJoinIdentity())
            return input;
        return new RandomValues<>(context, input, table);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpGroup groupBy, Iterator<BackendBindings<ID, VALUE>> input) {
        // TODO make it budget-based
        long limit = execCxt.getContext().getLong(RawConstants.LIMIT, 0L);
        if (limit <= 0L) {
            return input;
        }

        // execCxt.getContext().set(RawerConstants.LIMIT, (long) limit/2);
        for (int i = 0; i < groupBy.getAggregators().size(); ++i) {
            switch (groupBy.getAggregators().get(i).getAggregator()) {
                case AggCount ac -> {} // nothing, just checking it's handled (this is COUNT(*))
                // case AggCountVar acv -> {} // TODO count when (a) variable(s) is/are bound
                case AggCountVarDistinct acvd -> {}
                // case AggCountDistinct acd -> {} // nothing
                default -> throw new UnsupportedOperationException("The aggregation function is not implemented: " +
                        groupBy.getAggregators().get(i).toString());
            }
        }

        return new RandomAggregator<>(this, groupBy, input);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpLeftJoin leftJoin, Iterator<BackendBindings<ID, VALUE>> input) {
        Iterator<BackendBindings<ID, VALUE>> leftInput = ReturningArgsOpVisitorRouter.visit(this, leftJoin.getLeft(), input);
        return new RawOptional<>(leftInput, leftJoin.getRight(), execCxt);
    }
}
