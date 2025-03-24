package fr.gdd.passage.volcano.push;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutor;
import fr.gdd.passage.volcano.exceptions.PauseException;
import fr.gdd.passage.volcano.push.streams.*;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class PassagePushExecutor<ID,VALUE> extends ReturningArgsOpVisitor<
        PausableStream<ID,VALUE>,
        BackendBindings<ID,VALUE>>
        implements PassageExecutor<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final AtomicBoolean gotPaused = new AtomicBoolean(false);
    Boolean isDone = false;

    public PassagePushExecutor(PassageExecutionContext<ID,VALUE> context) {
        this.context = context;
        this.context.getContext().set(BackendConstants.EXECUTOR, this); // TODO, let a super do the job
    }

    @Override
    public Op execute(String query, Consumer<BackendBindings<ID, VALUE>> consumer) {
        return this.execute(Algebra.compile(QueryFactory.create(query)), consumer);
    }

    @Override
    public Op execute(Op root, Consumer<BackendBindings<ID, VALUE>> consumer) {
        root = context.optimizer.optimize(root);
        final Op _root = new DefaultGraphUriQueryModifier(context).visit(root);
        AtomicReference<PausableStream<ID,VALUE>> pausable = new AtomicReference<>();
        try (ForkJoinPool customPool = new ForkJoinPool(context.maxParallelism)) {
            customPool.submit(() -> {
                try {
                    pausable.set(this.visit(_root, new BackendBindings<>()));
                    pausable.get().stream().forEach(consumer);
                } catch (PauseException pe) {
                    gotPaused.set(true);
                }
            }).join();
        }

        isDone = true;

        return gotPaused.get() ?
                pausable.get().pause():
                null; // null means execution is over: we provided complete and correct results
    }

    public boolean gotPaused() {
        return gotPaused.get();
    }
    public boolean isDone() { return isDone; }

    /* *********************************** OPERATORS ************************************** */

    @Override
    public PausableStream<ID, VALUE> visit(OpTriple triple, BackendBindings<ID, VALUE> input) {
        return new PausableStreamWrapper<>(context, input, triple, SpliteratorScan::new);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpQuad quad, BackendBindings<ID, VALUE> input) {
        return  new PausableStreamWrapper<>(context, input, quad, SpliteratorScan::new);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpSlice slice, BackendBindings<ID, VALUE> input) {
        return new PausableStreamLimitOffset<>(context, input, slice);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpUnion union, BackendBindings<ID, VALUE> input) {
        return new PausableStreamWrapper<>(context, input, union, SpliteratorUnion::new);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpTable table, BackendBindings<ID, VALUE> input) {
        return new PausableStreamValues<>(context, input, table); // TODO split between BIND AS and VALUES
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpExtend extend, BackendBindings<ID, VALUE> input) {
        return new PausableStreamExtend<>(context, input, extend);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpJoin join, BackendBindings<ID, VALUE> input) {
        return new PausableStreamWrapper<>(context, input, join, SpliteratorJoin::new);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpProject project, BackendBindings<ID, VALUE> input) {
        return new PausableStreamProject<>(context, input, project);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpFilter filter, BackendBindings<ID, VALUE> input) {
        return new PausableStreamFilter<>(context, input, filter);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpLeftJoin lj, BackendBindings<ID, VALUE> input) {
        if (Objects.nonNull(lj.getExprs()) && !lj.getExprs().isEmpty()) {
            // throw new UnsupportedOperationException("Conditions in left joins are not handled yet.");
            // TODO actually modify the logical plan beforehand
            OpLeftJoin withoutExpr = OpLeftJoin.createLeftJoin(lj.getLeft(),
                    OpFilter.filterDirect(lj.getExprs(), lj.getRight()),
                    null);
            return this.visit(withoutExpr, input);
        }
        return new PausableStreamWrapper<>(context, input, lj, SpliteratorOptional::new);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpGroup groupBy, BackendBindings<ID, VALUE> input) {
        for (int i = 0; i < groupBy.getAggregators().size(); ++i) {
            switch (groupBy.getAggregators().get(i).getAggregator()) {
                case AggCount ignored -> {} // nothing, just checking it's handled (this is COUNT(*))
                case AggCountVar ignored -> {} // nothing, just checking it's handled (this is COUNT(?variable))
                case AggCountVarDistinct ignored -> throw new UnsupportedOperationException("COUNT DISTINCT with variable(s) is not supported.");
                case AggCountDistinct ignored -> throw new UnsupportedOperationException("COUNT DISTINCT of star (*) is not supported."); // TODO
                default -> throw new UnsupportedOperationException("The aggregation function is not implemented: " +
                        groupBy.getAggregators().get(i).toString());
            }
        }
        //        if (!groupBy.getGroupVars().isEmpty()) {
        //            throw new UnsupportedOperationException("Group keys are not supported.");
        //        }
        return new PausableStreamCount<>(context, input, groupBy);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpDistinct distinct, BackendBindings<ID, VALUE> input) {
        return new PausableStreamDistinct<>(context, input, distinct);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpService service, BackendBindings<ID, VALUE> input) {
        return new PausableStreamService<>(context, input, service);
    }
}
