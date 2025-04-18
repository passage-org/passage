package fr.gdd.passage.random.push;

import fr.gdd.passage.commons.engines.BackendPushExecutor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.random.push.streams.SpliteratorRawScan;
import fr.gdd.passage.random.push.streams.StreamJoin;
import fr.gdd.passage.random.push.streams.StreamRawRoot;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutor;
import fr.gdd.passage.volcano.exceptions.InvalidContexException;
import fr.gdd.passage.volcano.exceptions.PauseException;
import fr.gdd.passage.volcano.push.streams.PausableStream;
import fr.gdd.passage.volcano.push.streams.PausableStreamWrapper;
import org.apache.jena.sparql.algebra.Op;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class PassRawPushExecutor<ID,VALUE> extends BackendPushExecutor<ID,VALUE> implements PassageExecutor<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final AtomicBoolean gotPaused = new AtomicBoolean(false);
    Boolean isDone = false;

    public PassRawPushExecutor(PassageExecutionContext<ID,VALUE> ec) {
        super(ec,
                (c,i,o) -> new StreamRawRoot<>((PassageExecutionContext<ID, VALUE>) c, i, o),
                null,
                (c,i,o) -> new PausableStreamWrapper<>((PassageExecutionContext<ID, VALUE>) c, i, o, SpliteratorRawScan::new),
                (c,i,o) -> new PausableStreamWrapper<>((PassageExecutionContext<ID, VALUE>) c, i, o, SpliteratorRawScan::new),
                (c,i,o) -> new StreamJoin<>((PassageExecutionContext<ID, VALUE>) c, i, o),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
        this.context = ec;
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
                    pausable.set((PausableStream<ID, VALUE>) this.execute(_root, new BackendBindings<>()));
                    pausable.get().stream().forEach(consumer);
                } catch (PauseException pe) {
                    gotPaused.set(true);
                }
            }).join();
        }

        isDone = true;

        return pausable.get().pause(); // there always exists a continuation query for random walks
    }

}
