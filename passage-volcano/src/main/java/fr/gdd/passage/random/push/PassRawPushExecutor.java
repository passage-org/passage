package fr.gdd.passage.random.push;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.random.push.streams.SpliteratorRawRoot;
import fr.gdd.passage.random.push.streams.SpliteratorRawScan;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutor;
import fr.gdd.passage.volcano.exceptions.InvalidContexException;
import fr.gdd.passage.volcano.exceptions.PauseException;
import fr.gdd.passage.volcano.push.streams.PausableStream;
import fr.gdd.passage.volcano.push.streams.PausableStreamWrapper;
import fr.gdd.passage.volcano.push.streams.SpliteratorJoin;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Engine for RAW that uses random walks to sample the query, or to create approximate
 * responses. This engine uses streams to produce its bindings.
 */
public class PassRawPushExecutor<ID,VALUE> implements PassageExecutor<ID,VALUE>,
        ReturningArgsOpVisitor<PausableStream<ID,VALUE>,BackendBindings<ID,VALUE>> {

    final PassageExecutionContext<ID,VALUE> context;
    final AtomicBoolean gotPaused = new AtomicBoolean(false);
    Boolean isDone = false;

    public PassRawPushExecutor(PassageExecutionContext<ID,VALUE> context) {
        this.context = context;
        this.context.getContext().set(BackendConstants.EXECUTOR, this); // TODO, let a super do the job
        if (this.context.stoppingConditions.isEmpty()) {
            throw new InvalidContexException("The engine does not support running without stopping conditions");
        }
    }

    @Override
    public Op execute(Op root, Consumer<BackendBindings<ID, VALUE>> consumer) {
        root = context.optimizer.optimize(root); // TODO improve this…
        final Op _root = new DefaultGraphUriQueryModifier(context).visit(root);
        AtomicReference<PausableStream<ID,VALUE>> pausable = new AtomicReference<>();
        try (ForkJoinPool customPool = new ForkJoinPool(context.maxParallelism)) {
            customPool.submit(() -> {
                try {
                    pausable.set(this.execute(_root, new BackendBindings<>()));
                    pausable.get().stream().forEach(consumer);
                } catch (PauseException pe) {
                    gotPaused.set(true);
                }
            }).join();
        }

        isDone = true;

        return pausable.get().pause(); // there always exists a continuation query for random walks
    }

    @Override
    public Op execute(String query, Consumer<BackendBindings<ID, VALUE>> consumer) {
        return execute(Algebra.compile(QueryFactory.create(query)), consumer);
    }


    /* **************************** OPERATORS ************************************* */

    /**
     * @return The top most stream producing random walks guided by the query.
     */
    public PausableStream<ID,VALUE> execute(Op root, BackendBindings<ID, VALUE> input) {
        return new PausableStreamWrapper<>(context, input, root, SpliteratorRawRoot::new);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpTriple triple, BackendBindings<ID, VALUE> input) {
        return new PausableStreamWrapper<>(context, input, triple, SpliteratorRawScan::new, true);
    }

    @Override
    public PausableStream<ID, VALUE> visit(OpJoin join, BackendBindings<ID, VALUE> input) {
        return new PausableStreamWrapper<>(context, input, join, SpliteratorJoin::new); // already enable back jump TODO more generic
    }
}
