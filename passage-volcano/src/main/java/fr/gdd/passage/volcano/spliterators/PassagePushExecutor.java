package fr.gdd.passage.volcano.spliterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.pause.PauseException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PassagePushExecutor<ID,VALUE> extends ReturningArgsOpVisitor<
        Stream<BackendBindings<ID, VALUE>>, // output
        BackendBindings<ID, VALUE>> { // input

    final PassageExecutionContext context;

    public PassagePushExecutor (PassageExecutionContext context) {
        this.context = context;
        this.context.getContext().set(BackendConstants.EXECUTOR, this);
    }

    public Stream<BackendBindings<ID, VALUE>> execute(Op root) {
        root = context.optimizer.optimize(root);
        root = new DefaultGraphUriQueryModifier(context).visit(root);
        context.setQuery(root); // mandatory to be saved later on
        // Only need a root that will catch the timeout exception to state that
        // the iterator does not have next.
        return this.visit(root, new BackendBindings<>());
    }

    public void execute(Op root, Consumer<BackendBindings<ID, VALUE>> consumer) {
        // With a timeout condition, we need to create a catch that wraps the
        // whole process.
        try (ForkJoinPool customPool = new ForkJoinPool(4)) { // TODO parallelism factor is a parameter
            customPool.submit(() -> {
                try {
                    execute(root).forEach(consumer);
                } catch (PauseException pe) {
                    // TODO test if there are multiple PauseException catch.
                    //      The best scenario would be that children continue
                    //      their execution until throwing. So when join is called
                    //      they are all stopped in a consistent state.
                    System.out.println("Meow");
                }
            }).join();
        }
    }

    /* *********************************** OPERATORS ************************************* */

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpTriple triple, BackendBindings<ID, VALUE> input) {
        var meow = new PassageExecutionContextBuilder().setContext(context);
        return StreamSupport.stream(new PassageSplitScan<>(meow.build().setLimit(null).setOffset(0L), input, triple), true);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpJoin join, BackendBindings<ID, VALUE> input) {
         return this.visit(join.getLeft(), input).flatMap(b -> this.visit(join.getRight(), b));
    }

}
