package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;

public class SpliteratorJoin<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>> {

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final PassagePushExecutor<ID,VALUE> executor;
    final OpJoin join;
    PausableStream<ID,VALUE> leftStream;
    final Spliterator<BackendBindings<ID,VALUE>> left;
    PausableStream<ID,VALUE> right;
    Spliterator<BackendBindings<ID,VALUE>> rightSplit;

    ConcurrentHashMap<Long, SpliteratorJoin<ID,VALUE>> joins = new ConcurrentHashMap<>();
    final static AtomicLong ids = new AtomicLong();
    final long id;

    public SpliteratorJoin(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpJoin join) {
        this.context = context;
        this.input = input;
        this.join = join;
        this.executor = (PassagePushExecutor<ID, VALUE>) context.executor;
        this.leftStream = executor.visit(join.getLeft(), input);
        this.left = leftStream.stream().spliterator();
        this.id = ids.incrementAndGet();
        register(joins);
    }

    public SpliteratorJoin(Spliterator<BackendBindings<ID,VALUE>> left, PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpJoin join) {
        this.context = context;
        this.input = input;
        this.join = join;
        this.executor = (PassagePushExecutor<ID, VALUE>) context.executor;
        this.left = left;
        this.id = ids.incrementAndGet();
        register(joins);
    }

    public SpliteratorJoin<ID,VALUE> register (ConcurrentHashMap<Long, SpliteratorJoin<ID,VALUE>> joins) {
        this.joins = joins;
        this.joins.put(id, this);
        return this;
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        // we do not use a simple `.forEach(action)` on left stream because
        // it fully consumes the sub-stream, which contradict the goal of lazy
        // mapping-at-a-time evaluation.
        // Instead, we `tryAdvance` like a regular spliterator.
        if (Objects.nonNull(rightSplit)) {
            if (rightSplit.tryAdvance(action)) {
                return true;
            } else {
                rightSplit = null;
            }
        }

        if (left.tryAdvance(b -> {
            right = executor.visit(join.getRight(), b);
            rightSplit = right.stream().spliterator();
        })) {
            return this.tryAdvance(action); // rightsplit updated, try anew.
        }

        return false;
    }

    @Override
    public Spliterator<BackendBindings<ID, VALUE>> trySplit() {
        Spliterator<BackendBindings<ID, VALUE>> split = left.trySplit();
        if (Objects.isNull(split)) return null; // not possible

        return new SpliteratorJoin<>(split, context, input, join).register(joins);
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return left.characteristics();
    }

    /* ************************************ PAUSE ********************************** */

    public Op pause () {
        Op pausedLeft = leftStream.pause();

        Op pausedRight = joins.values().stream().map(SpliteratorJoin::pauseRight).filter(Objects::nonNull).reduce(DONE, removeEmptyOfUnion);

        if (isDone(pausedLeft) && isDone(pausedRight)) { return DONE; }
        if (isDone(pausedRight)) { return OpJoin.create(pausedLeft, join.getRight()); }

        // no state needed really, everything is in the returned value of these:
        // Op pausedRight = rights.values().stream().map(PausableStream::pause).reduce(DONE, removeEmptyOfUnion);

        // not executed at all, we return it pristine
        if (notExecuted(pausedLeft, join.getLeft()) && notExecuted(pausedRight, join.getRight())) return join;

        if (isDone(pausedLeft) && isDone(pausedRight)) return DONE; // both done, so we are done
        if (isDone(pausedLeft) && notExecuted(pausedRight, join.getRight())) return DONE; // consumed left but right did not move, so the join is done
        // all consumed left or kept still, but generating a new right to consume
        if (isDone(pausedLeft) || notExecuted(pausedLeft, join.getLeft())) return pausedRight;
        // still must do the rest of left with the old right
        if (isDone(pausedRight) || notExecuted(pausedRight, join.getRight())) return OpJoin.create(pausedLeft, join.getRight());

        // Otherwise, create a union with:
        // (i) The preempted right part (The current pointer to where we are executing)
        // (ii) The preempted left part with a copy of the right (The rest of the query)
        // In other words, it's like, (i) finish the OFFSET you where in. (ii) start at OFFSET + 1
        return OpUnion.create(pausedRight, OpJoin.create(pausedLeft, join.getRight()));
    }

    public Op pauseRight () {
        return Objects.isNull(right) ? null : this.right.pause();
    }
}
