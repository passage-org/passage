package fr.gdd.sage.rawer;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.BackendSaver;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.iterators.SagerBind;
import fr.gdd.sage.rawer.accumulators.AccumulatorFactory;
import fr.gdd.sage.rawer.budgeting.NaiveBudgeting;
import fr.gdd.sage.rawer.iterators.ProjectIterator;
import fr.gdd.sage.rawer.iterators.RandomAggregator;
import fr.gdd.sage.rawer.iterators.RandomRoot;
import fr.gdd.sage.rawer.iterators.RandomScanFactory;
import fr.gdd.sage.sager.optimizers.CardinalityJoinOrdering;
import fr.gdd.sage.sager.pause.Triples2BGP;
import fr.gdd.sage.sager.resume.BGP2Triples;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.Iterator;

/**
 * Execute the query and exactly the query that has been asked.
 * If an operator is not implemented, then it returns the explicit mention
 * that it's not implemented. No surprises.
 */
public class RawerOpExecutor<ID, VALUE> extends ReturningArgsOpVisitor<
        Iterator<BackendBindings<ID, VALUE>>, // input
        Iterator<BackendBindings<ID, VALUE>>> { // output

    final ExecutionContext execCxt;
    Backend<ID, VALUE, ?> backend;
    CacheId<ID, VALUE> cache;

    public RawerOpExecutor() {
        // This creates a brandnew execution context, but it's important
        // that `setBackend` is called, or it will throw at runtime.
        this.execCxt = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        execCxt.getContext().setIfUndef(RawerConstants.SCANS, 0L);
        execCxt.getContext().setIfUndef(RawerConstants.LIMIT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(RawerConstants.TIMEOUT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(RawerConstants.BUDGETING, new NaiveBudgeting(
                execCxt.getContext().get(RawerConstants.TIMEOUT),
                execCxt.getContext().get(RawerConstants.LIMIT)));
    }

    public RawerOpExecutor(ExecutionContext execCxt) {
        // TODO default execution context to unburden this class
        this.execCxt = execCxt;
        this.backend = execCxt.getContext().get(RawerConstants.BACKEND);
        execCxt.getContext().setIfUndef(RawerConstants.SCANS, 0L);
        execCxt.getContext().setIfUndef(RawerConstants.LIMIT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(RawerConstants.TIMEOUT, Long.MAX_VALUE);
        this.cache = new CacheId<>(this.backend);
        execCxt.getContext().setIfUndef(RawerConstants.CACHE, cache);
        execCxt.getContext().setIfUndef(RawerConstants.BUDGETING, new NaiveBudgeting(
                execCxt.getContext().get(RawerConstants.TIMEOUT),
                execCxt.getContext().get(RawerConstants.LIMIT)));
    }

    public RawerOpExecutor<ID, VALUE> setTimeout(Long timeout) {
        execCxt.getContext().set(RawerConstants.TIMEOUT, timeout);
        long deadline = System.currentTimeMillis() + timeout;
        execCxt.getContext().set(RawerConstants.DEADLINE, deadline > 0 ? deadline : Long.MAX_VALUE); // handle overflow
        return this;
    }

    public RawerOpExecutor<ID, VALUE> setLimit(Long limit) {
        execCxt.getContext().set(RawerConstants.LIMIT, limit);
        return this;
    }

    public RawerOpExecutor<ID, VALUE> setCache(CacheId<ID,VALUE> cache) {
        execCxt.getContext().set(RawerConstants.CACHE, cache);
        this.cache = cache;
        return this;
    }

    public RawerOpExecutor<ID,VALUE> setBackend(Backend<ID,VALUE,?> backend) {
        this.backend = backend;
        execCxt.getContext().set(RawerConstants.BACKEND, backend);
        execCxt.getContext().setIfUndef(RawerConstants.CACHE, new CacheId<>(this.backend));
        this.cache = execCxt.getContext().get(RawerConstants.CACHE);
        return this;
    }

    public RawerOpExecutor<ID,VALUE> setMaxThreads(int maxThreads) {
        execCxt.getContext().set(RawerConstants.MAX_THREADS, maxThreads);
        return this;
    }

    public RawerOpExecutor<ID,VALUE> forceOrder() {
        execCxt.getContext().setTrue(RawerConstants.FORCE_ORDER);
        return this;
    }

    /**
     * Depending on the backend, it might be profitable to use another count-distinct
     * algorithm. By default, it's crawd.
     * @param factory The factory that create the approximate accumulators.
     * @return this, for convenience.
     */
    public RawerOpExecutor<ID,VALUE> setCountDistinct(AccumulatorFactory<ID,VALUE> factory) {
        execCxt.getContext().set(RawerConstants.COUNT_DISTINCT_FACTORY, factory);
        return this;
    }

    public Backend<ID, VALUE, ?> getBackend() {
        return backend;
    }
    public ExecutionContext getExecutionContext() {
        return execCxt;
    }
    public CacheId<ID, VALUE> getCache() { return cache;}

    /* ************************************************************************ */

    public Iterator<BackendBindings<ID,VALUE>> execute(String queryAsString) {
        return this.execute(Algebra.compile(QueryFactory.create(queryAsString)));
    }

    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        // #A reordering of bgps if need be
        if (execCxt.getContext().isFalseOrUndef(RawerConstants.FORCE_ORDER)) {
            root = ReturningOpVisitorRouter.visit(new Triples2BGP(), root);
            root = new CardinalityJoinOrdering<>(backend, cache).visit(root); // need to have bgp to optimize, no tps
        }
        root = ReturningOpVisitorRouter.visit(new BGP2Triples(), root);

        execCxt.getContext().set(RawerConstants.SAVER, new BackendSaver<>(backend, root));
        return new RandomRoot<>(this, execCxt, root);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple triple, Iterator<BackendBindings<ID, VALUE>> input) {
        return new RandomScanFactory<>(input, execCxt, triple);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        return new ProjectIterator<>(project, ReturningArgsOpVisitorRouter.visit(this, project.getSubOp(), input));
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpExtend extend, Iterator<BackendBindings<ID, VALUE>> input) {
        // TODO throw when the expressions inside the OpExtend are not supported
        CacheId<ID,VALUE> cache = execCxt.getContext().get(RawerConstants.CACHE);
        Iterator<BackendBindings<ID, VALUE>> wrapped = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
        return new SagerBind<>(wrapped, extend, backend, cache, execCxt);
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
        throw new UnsupportedOperationException("TODO: VALUES…"); // TODO
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpGroup groupBy, Iterator<BackendBindings<ID, VALUE>> input) {
        // TODO make it budget-based
        long limit = execCxt.getContext().getLong(RawerConstants.LIMIT, 0L);
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

}
