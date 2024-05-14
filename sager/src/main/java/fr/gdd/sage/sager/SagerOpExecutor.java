package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.iterators.SagerRoot;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import fr.gdd.sage.sager.optimizers.SagerOptimizer;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

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

    public SagerOpExecutor(ExecutionContext execCxt) {
        this.execCxt = execCxt;
        this.backend = execCxt.getContext().get(SagerConstants.BACKEND);
        execCxt.getContext().setIfUndef(SagerConstants.SCANS, 0L);
        execCxt.getContext().setIfUndef(SagerConstants.LIMIT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(SagerConstants.TIMEOUT, Long.MAX_VALUE);

        // as setifundef so outsiders can configure their own list of optimizers
        execCxt.getContext().setIfUndef(SagerConstants.LOADER, new SagerOptimizer());
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

//    /**
//     * @param root The query to execute in the form of a Jena `Op`.
//     * @return An iterator that can produce the bindings.
//     */
//    public QueryIterator optimizeThenExecute(Op root) {
//        SagerOptimizer optimizer = execCxt.getContext().get(SagerConstants.LOADER);
//        root = optimizer.optimize(root);
//        return this.execute(root);
//    }

    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        execCxt.getContext().set(SagerConstants.SAVER, new Save2SPARQL(root, execCxt));

        Iterator<BackendBindings<ID, VALUE>> wrapped = new SagerRoot<>(execCxt,
                ReturningArgsOpVisitorRouter.visit(this, root, Iter.of(new BackendBindings<>())));

        return wrapped;
//        return QueryIterPlainWrapper.create(Iter.map(wrapped, bnid -> {
//            BindingBuilder builder = BindingFactory.builder();
//            for (Var var : bnid) {
//                builder.add(var, bnid.getValue(var));
//            }
//            return builder.build();
//        }), execCxt);
    }

    /* ******************************************************************* */

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple opTriple, Iterator<BackendBindings<ID, VALUE>> input) {
        return new SagerScanFactory(input, execCxt, opTriple);
    }

//    @Override
//    public Iterator<BindingId2Value> visit(OpSequence sequence, Iterator<BindingId2Value> input) {
//        for (Op op : sequence.getElements()) {
//            input = ReturningArgsOpVisitorRouter.visit(this, op, input);
//        }
//        return input;
//    }
//
//    @Override
//    public Iterator<BindingId2Value> visit(OpJoin join, Iterator<BindingId2Value> input) {
//        input = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), input);
//        return ReturningArgsOpVisitorRouter.visit(this, join.getRight(), input);
//    }
//
//    @Override
//    public Iterator<BindingId2Value> visit(OpUnion union, Iterator<BindingId2Value> input) {
//        // TODO What about some parallelism here? :)
//        Save2SPARQL saver = execCxt.getContext().get(SagerConstants.SAVER);
//        SagerUnion iterator = new SagerUnion(this, input, union.getLeft(), union.getRight());
//        saver.register(union, iterator);
//        return iterator;
//    }
//
//    @Override
//    public Iterator<BindingId2Value> visit(OpExtend extend, Iterator<BindingId2Value> input) {
//        Iterator<BindingId2Value> newInput = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
//        return new SagerBind(newInput, extend, execCxt);
//    }
//
//    @Override
//    public Iterator<BindingId2Value> visit(OpTable table, Iterator<BindingId2Value> input) {
//        if (table.isJoinIdentity())
//            return input;
//        throw new UnsupportedOperationException("TODO: VALUES Should be considered as a Scan iterator…"); // TODO
//    }
//
//    /**
//     * Preemption mostly comes from this: the ability to start over from an OFFSET efficiently.
//     * When we find a pattern like SELECT * WHERE {?s ?p ?o} OFFSET X, the engine know that
//     * it must skip X elements of the iterator. But the pattern must be accurate: a single
//     * triple pattern.
//     */
//    @Override
//    public Iterator<BindingId2Value> visit(OpSlice slice, Iterator<BindingId2Value> input) {
//        if (slice.getSubOp() instanceof OpTriple triple) { // TODO OpQuad
//            return new SagerScanFactory(input, execCxt, triple, slice.getStart());
//        }
//        // TODO otherwise it's a normal slice
//        throw new UnsupportedOperationException("TODO Default LIMIT OFFSET not implemented yet.");
//    }

}
