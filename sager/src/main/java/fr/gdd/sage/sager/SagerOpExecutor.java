package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.iterators.SagerBind;
import fr.gdd.sage.sager.iterators.*;
import fr.gdd.sage.sager.optimizers.Progress;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import fr.gdd.sage.sager.resume.IsSkippable;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

/**
 * Execute only operators that can be preempted. Operators work
 * on identifiers by default instead of values, for the sake of performance.
 * That's why it does not extend `OpExecutor` since the latter
 * works on `QueryIterator` that returns `Binding` that provides `Node`.
 */
public class SagerOpExecutor<ID, VALUE> extends ReturningArgsOpVisitor<
        Iterator<BackendBindings<ID, VALUE>>, // input
        Iterator<BackendBindings<ID, VALUE>>> { // output

    final ExecutionContext execCxt;
    final Backend<ID, VALUE, Long> backend;
    final CacheId<ID,VALUE> cache;

    public SagerOpExecutor(ExecutionContext execCxt) {
        this.execCxt = execCxt;
        this.backend = execCxt.getContext().get(SagerConstants.BACKEND);
        execCxt.getContext().setIfUndef(SagerConstants.SCANS, 0L);
        execCxt.getContext().setIfUndef(SagerConstants.LIMIT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(SagerConstants.TIMEOUT, Long.MAX_VALUE);
        this.cache = new CacheId<>(backend);
        execCxt.getContext().setIfUndef(SagerConstants.CACHE, this.cache);
        execCxt.getContext().setFalse(SagerConstants.PAUSED);

        // as setifundef so outsiders can configure their own list of optimizers
        execCxt.getContext().setIfUndef(SagerConstants.LOADER, new SagerOptimizer());
    }

    public Backend<ID, VALUE, Long> getBackend() {
        return backend;
    }

    public ExecutionContext getExecutionContext() {
        return execCxt;
    }

    public SagerOpExecutor<ID, VALUE> setTimeout(Long timeout) {
        execCxt.getContext().set(SagerConstants.TIMEOUT, timeout);
        execCxt.getContext().set(SagerConstants.DEADLINE, System.currentTimeMillis()+timeout);
        return this;
    }

    public SagerOpExecutor<ID, VALUE> setLimit(Long limit) {
        execCxt.getContext().set(SagerConstants.LIMIT, limit);
        return this;
    }

    /**
     * @param root The query to execute in the form of a Jena `Op`.
     * @return An iterator that can produce the bindings.
     */
    public Iterator<BackendBindings<ID,VALUE>> optimizeThenExecute(Op root) {
        SagerOptimizer optimizer = execCxt.getContext().get(SagerConstants.LOADER);
        root = optimizer.optimize(root);
        return this.execute(root);
    }

    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        execCxt.getContext().set(SagerConstants.SAVER, new Save2SPARQL<ID,VALUE>(root, execCxt));

        return new SagerRoot<>(execCxt,
                ReturningArgsOpVisitorRouter.visit(this, root, Iter.of(new BackendBindings<>())));
    }

    public String pauseAsString () {
        Op paused = pause();
        return Objects.isNull(paused) ? null : OpAsQuery.asQuery(paused).toString();
    }

    public Op pause() {
        execCxt.getContext().setTrue(SagerConstants.PAUSED);
        Save2SPARQL<ID, VALUE> saver = execCxt.getContext().get(SagerConstants.SAVER);
        return saver.save(null);
    }

    public double progress() {
        Progress progress = new Progress(execCxt.getContext().get(SagerConstants.SAVER));
        return progress.get();
    }

    /* ******************************************************************* */

    public Iterator<BackendBindings<ID, VALUE>> visit(Op op, Iterator<BackendBindings<ID,VALUE>> input) {
        return ReturningArgsOpVisitorRouter.visit(this, op, input); // only routing
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        return new SagerProject<>(this, project, input);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpDistinct distinct, Iterator<BackendBindings<ID, VALUE>> input) { // see QueryIterDistinct
        return new SagerDistinct<>(distinct, execCxt, ReturningArgsOpVisitorRouter.visit(this, distinct.getSubOp(), input));
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple opTriple, Iterator<BackendBindings<ID, VALUE>> input) {
        return new SagerScanFactory<>(input, execCxt, opTriple);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpFilter filter, Iterator<BackendBindings<ID, VALUE>> input) { // see QueryIterFilterExpr
        return new SagerFilter<>(this, filter, ReturningArgsOpVisitorRouter.visit(this, filter.getSubOp(), input));
    }

    //    @Override
//    public Iterator<BindingId2Value> visit(OpSequence sequence, Iterator<BindingId2Value> input) {
//        for (Op op : sequence.getElements()) {
//            input = ReturningArgsOpVisitorRouter.visit(this, op, input);
//        }
//        return input;
//    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpJoin join, Iterator<BackendBindings<ID,VALUE>> input) {
        input = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), input);
        return ReturningArgsOpVisitorRouter.visit(this, join.getRight(), input);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpUnion union, Iterator<BackendBindings<ID,VALUE>> input) {
        // TODO What about some parallelism here? :)
        Save2SPARQL<ID,VALUE> saver = execCxt.getContext().get(SagerConstants.SAVER);
        SagerUnion<ID,VALUE> iterator = new SagerUnion<>(this, input, union.getLeft(), union.getRight());
        saver.register(union, iterator);
        return iterator;
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpExtend extend, Iterator<BackendBindings<ID,VALUE>> input) {
        Iterator<BackendBindings<ID,VALUE>> newInput = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
        return new SagerBind<>(newInput, extend, backend, cache, execCxt);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpTable table, Iterator<BackendBindings<ID,VALUE>> input) {
        if (table.isJoinIdentity())
            return input;
        throw new UnsupportedOperationException("TODO: VALUES Should be considered as a Scan iterator…"); // TODO
    }

    /**
     * Preemption mostly comes from this: the ability to start over from an OFFSET efficiently.
     * When we find a pattern like SELECT * WHERE {?s ?p ?o} OFFSET X, the engine know that
     * it must skip X elements of the iterator. But the pattern must be accurate: a single
     * triple pattern.
     */
    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpSlice slice, Iterator<BackendBindings<ID, VALUE>> input) {
        Boolean isSkippable = ReturningOpVisitorRouter.visit(new IsSkippable(), slice);

        if (isSkippable) {
            PreemptedSubQueryOpExecutor<ID,VALUE> subExec = new PreemptedSubQueryOpExecutor<>(execCxt, backend);
            return ReturningArgsOpVisitorRouter.visit(subExec, slice, input);
        }
        // TODO otherwise it's a normal slice (TODO) handle it
        throw new UnsupportedOperationException("TODO Default LIMIT OFFSET not implemented yet.");
    }


    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpConditional cond, Iterator<BackendBindings<ID, VALUE>> input) {
        return new SagerOptional<>(this, cond, input);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpLeftJoin lj, Iterator<BackendBindings<ID,VALUE>> input) {
        if (Objects.isNull(lj.getExprs()) || lj.getExprs().isEmpty()) {
            return new SagerOptional<>(this, lj, input);
        }
        throw new UnsupportedOperationException("Left join with embedded expression(s) is not handled yet.");
    }

        @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpGroup groupBy, Iterator<BackendBindings<ID, VALUE>> input) {
//        if (!groupBy.getGroupVars().isEmpty()) {
//            throw new UnsupportedOperationException("Group by not handled (yet).");
//        }

        if (groupBy.getAggregators().size() > 1) {
            throw new UnsupportedOperationException("Only one aggregator supported for now.");
        }

        return new SagerAgg<>(this, groupBy, input);
   }

}
