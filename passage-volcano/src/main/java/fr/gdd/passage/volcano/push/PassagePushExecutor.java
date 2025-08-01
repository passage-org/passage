package fr.gdd.passage.volcano.push;

import fr.gdd.passage.commons.engines.BackendPushExecutor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutor;
import fr.gdd.passage.volcano.exceptions.PauseException;
import fr.gdd.passage.volcano.push.streams.*;
import org.apache.jena.sparql.algebra.Op;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class PassagePushExecutor<ID,VALUE> extends BackendPushExecutor<ID,VALUE> implements PassageExecutor<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final AtomicBoolean gotPaused = new AtomicBoolean(false);
    Boolean isDone = false;

    public PassagePushExecutor(PassageExecutionContext<ID,VALUE> ec) {
        super(ec,
                (c,i,o) -> new PausableStreamRoot<>((PassageExecutionContext<ID, VALUE>) c, i, o),
                (c,i,o) -> new PausableStreamProject<>((PassageExecutionContext<ID, VALUE>) c, i, o),
                (c,i,o) -> new PausableStreamWrapper<>((PassageExecutionContext<ID, VALUE>) c, i, o, SpliteratorScan::new),
                (c,i,o) -> new PausableStreamWrapper<>((PassageExecutionContext<ID, VALUE>) c, i, o, SpliteratorScan::new),
                (c,i,o) -> new PausableStreamWrapper<>((PassageExecutionContext<ID, VALUE>) c, i, o, SpliteratorJoin::new),
                (c,i,o) -> new PausableStreamWrapper<>((PassageExecutionContext<ID, VALUE>) c, i, o, SpliteratorUnion::new),
                (c,i,o) -> new PausableStreamValues<>((PassageExecutionContext<ID, VALUE>) c, i, o),
                (c,i,o) -> new PausableStreamExtend<>((PassageExecutionContext<ID, VALUE>) c, i, o),
                (c,i,o) -> new PausableStreamFilter<>((PassageExecutionContext<ID,VALUE>) c, i, o ),
                (c,i,o) -> new PausableStreamDistinct<>((PassageExecutionContext<ID, VALUE>) c, i, o ),
                (c,i,o) -> new PausableStreamLimitOffset<>((PassageExecutionContext<ID, VALUE>) c, i, o ),
                (c,i,o) -> new PausableStreamWrapper<>((PassageExecutionContext<ID,VALUE>) c, i, o, SpliteratorOptional::new),
                (c,i,o) -> new PausableStreamCount<>((PassageExecutionContext<ID, VALUE>) c, i, o ),
                (c,i,o) -> new PausableStreamService<>((PassageExecutionContext<ID,VALUE>) c, i, o)
        );
        this.context = ec;
    }

    @Override
    public Op execute(Op root, Consumer<BackendBindings<ID, VALUE>> consumer) {
        root = context.optimizer.optimize(root);
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

        return gotPaused.get() ?
                Pause2Continuation.removeDuplicatedFilters(pausable.get().pause()):
                null; // null means execution is over: we provided complete and correct results
    }

}
