package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;
import static fr.gdd.passage.volcano.push.Pause2Continuation.isDone;

public class SpliteratorUnion<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>>, PausableSpliterator<ID,VALUE> {

    OpUnion union;
    PausableStream<ID,VALUE> left;
    Spliterator<BackendBindings<ID,VALUE>> leftSplit;
    PausableStream<ID,VALUE> right;
    Spliterator<BackendBindings<ID,VALUE>> rightSplit;
    SpliteratorUnion<ID,VALUE> sibling;

    public SpliteratorUnion(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpUnion union) {
        this.union = union;
        var executor = (PassagePushExecutor<ID, VALUE>) context.executor;
        this.left = executor.visit(union.getLeft(), input);
        this.leftSplit = this.left.stream().spliterator();
        this.right = executor.visit(union.getRight(), input);
        this.rightSplit = this.right.stream().spliterator();
    }

    public SpliteratorUnion(PausableStream<ID,VALUE> right, Spliterator<BackendBindings<ID,VALUE>> rightSplit) {
        this.right = right;
        this.rightSplit = rightSplit;
        // the rest do not need initializing
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.nonNull(leftSplit)) {
            if (leftSplit.tryAdvance(action)) {
                return true;
            }
        }
        if (Objects.nonNull(rightSplit)) {
            return rightSplit.tryAdvance(action);
        }

        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        if (Objects.isNull(left) || Objects.isNull(right)) {
            return null; // already split
        }
        // keep left for ourselves

        // give right to other union
        sibling = new SpliteratorUnion<>(this.right, this.rightSplit);
        this.right = null;
        this.rightSplit = null;

        return sibling;
    }

    @Override
    public long estimateSize() {
        long leftSize = Objects.nonNull(leftSplit) ? leftSplit.estimateSize() : 0;
        long rightSize = Objects.nonNull(rightSplit) ? rightSplit.estimateSize() : 0;
        return leftSize + rightSize;
    }

    @Override
    public int characteristics() {
        return CONCURRENT;
    }

    @Override
    public Op pause() {
        Op pausedRight = Objects.nonNull(sibling) ? sibling.right.pause() : right.pause();
        Op pausedLeft = left.pause();

        if (notExecuted(pausedLeft, union.getLeft()) && notExecuted(pausedRight, union.getRight())) return union;

        if (isDone(pausedLeft) && isDone(pausedRight)) return DONE;
        if (isDone(pausedLeft)) return pausedRight;
        if (isDone(pausedRight)) return pausedLeft;

        return OpUnion.create(pausedLeft, pausedRight);
    }
}
