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

/**
 * Aims to implement our own choices about parallelism, i.e., `trySplit` should
 * be prioritized over scans' `trySplit` because scans potentially create new `UNION`
 * clauses.
 */
public class SpliteratorUnion<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>>, PausableSpliterator<ID,VALUE> {

    OpUnion union;
    PausableStream<ID,VALUE> main; // left when not parallel
    final Spliterator<BackendBindings<ID,VALUE>> mainSplit;
    PausableStream<ID,VALUE> secondary; // right when not parallel
    Spliterator<BackendBindings<ID,VALUE>> secondarySplit;

    public SpliteratorUnion(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpUnion union) {
        this.union = union;
        var executor = (PassagePushExecutor<ID, VALUE>) context.executor;
        this.main = executor.visit(union.getLeft(), input);
        this.mainSplit = this.main.stream().spliterator();
        this.secondary = executor.visit(union.getRight(), input);
        this.secondarySplit = this.secondary.stream().spliterator();
    }

    public SpliteratorUnion(Spliterator<BackendBindings<ID,VALUE>> mainSplit) {
        this.mainSplit = mainSplit;
        // the rest do not need initializing
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.nonNull(mainSplit)) {
            if (mainSplit.tryAdvance(action)) {
                return true;
            }
        }
        if (Objects.nonNull(secondarySplit)) {
            return secondarySplit.tryAdvance(action);
        }

        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        if (Objects.isNull(secondarySplit)) {
            // Has already been split,
            Spliterator<BackendBindings<ID,VALUE>> newSplit = mainSplit.trySplit();
            if (Objects.nonNull(newSplit)) {
                return new SpliteratorUnion<>(newSplit);
            }
            return null; // already split, and main does not allow, so we are done.
        }

        // otherwise, we split the in two sides
        // give right to other union
        SpliteratorUnion<ID,VALUE> newSplit = new SpliteratorUnion<>(this.secondarySplit);
        this.secondarySplit = null;

        return newSplit;
    }

    @Override
    public long estimateSize() {
        long leftSize = Objects.nonNull(mainSplit) ? mainSplit.estimateSize() : 0;
        long rightSize = Objects.nonNull(secondarySplit) ? secondarySplit.estimateSize() : 0;
        return leftSize + rightSize;
    }

    @Override
    public int characteristics() {
        return CONCURRENT;
    }

    @Override
    public Op pause() {
        Op pausedLeft = main.pause();
        Op pausedRight = secondary.pause();

        if (notExecuted(pausedLeft, union.getLeft()) && notExecuted(pausedRight, union.getRight())) return union;

        if (isDone(pausedLeft) && isDone(pausedRight)) return DONE;
        if (isDone(pausedLeft)) return pausedRight;
        if (isDone(pausedRight)) return pausedLeft;

        return OpUnion.create(pausedLeft, pausedRight);
    }

}
