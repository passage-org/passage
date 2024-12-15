package fr.gdd.passage.volcano.push;

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
        AtomicBoolean gotPaused = new AtomicBoolean(false);
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
        return gotPaused.get() ?
                pausable.get().pause():
                null; // null means execution is over: we provided complete and correct results
    }

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
        return new PausableStreamUnion<>(context, input, union);
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
            throw new UnsupportedOperationException("Conditions in left joins are not handled yet.");
        }
        return new PausableStreamWrapper<>(context, input, lj, SpliteratorOptional::new);
    }
}
