package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.stream.Stream;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;

public class PausableStreamUnion<ID,VALUE> implements PausableStream<ID,VALUE> {

    final OpUnion union;
    final PausableStream<ID,VALUE> left;
    final PausableStream<ID,VALUE> right;
    final PassagePushExecutor<ID,VALUE> executor;

    public PausableStreamUnion(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpUnion union) {
        this.union = union;
        this.executor = (PassagePushExecutor<ID, VALUE>) context.executor;
        this.left = this.executor.visit(union.getLeft(), input);
        this.right = this.executor.visit(union.getRight(), input);
    }


    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return Stream.concat(left.stream(), right.stream());
    }

    @Override
    public Op pause() {
        Op pausedLeft = left.pause();
        Op pausedRight = right.pause();

        if (notExecuted(pausedLeft, union.getLeft()) && notExecuted(pausedRight, union.getRight())) return union;

        if (isDone(pausedLeft) && isDone(pausedRight)) return DONE;
        if (isDone(pausedLeft)) return pausedRight;
        if (isDone(pausedRight)) return pausedLeft;

        return OpUnion.create(pausedLeft, pausedRight);
    }
}
