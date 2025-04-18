package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.engines.BackendPushExecutor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.Pause2Continuation;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static fr.gdd.passage.volcano.push.Pause2Continuation.DONE;
import static fr.gdd.passage.volcano.push.Pause2Continuation.removeEmptyOfUnion;

public class SpliteratorOptional<ID,VALUE> implements Spliterator<BackendBindings<ID,VALUE>>, PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final BackendPushExecutor<ID,VALUE> executor;
    final OpLeftJoin lj;
    PausableStream<ID,VALUE> leftStream;
    final Spliterator<BackendBindings<ID,VALUE>> left;
    PausableStream<ID,VALUE> right;
    Spliterator<BackendBindings<ID,VALUE>> rightSplit;

    boolean matchOptional = false;
    BackendBindings<ID,VALUE> inputOfOptional = null; // TODO maybe a atomic ref
    ConcurrentHashMap<Long, SpliteratorOptional<ID,VALUE>> optionals = new ConcurrentHashMap<>();
    final static AtomicLong ids = new AtomicLong();
    final long id;

    public SpliteratorOptional(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpLeftJoin lj) {
        this.context = context;
        this.input = input;
        this.lj = lj;
        this.executor = (BackendPushExecutor<ID, VALUE>) context.executor;
        this.leftStream = (PausableStream<ID, VALUE>) executor.visit(lj.getLeft(), input);
        this.left = leftStream.stream().spliterator();
        this.id = ids.incrementAndGet();
        register(optionals);
    }

    public SpliteratorOptional(Spliterator<BackendBindings<ID,VALUE>> left, PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpLeftJoin lj) {
        this.context = context;
        this.input = input;
        this.lj = lj;
        this.executor = context.executor;
        this.left = left;
        this.id = ids.incrementAndGet();
        register(optionals);
    }

    public SpliteratorOptional<ID,VALUE> register (ConcurrentHashMap<Long, SpliteratorOptional<ID,VALUE>> joins) {
        this.optionals = joins;
        this.optionals.put(id, this);
        return this;
    }

    @Override
    public boolean tryAdvance(Consumer<? super BackendBindings<ID, VALUE>> action) {
        if (Objects.nonNull(rightSplit)) {
            if (rightSplit.tryAdvance(action)) {
                matchOptional = true; // state needed for pause
                return true;
            } else {
                if (!matchOptional) {
                    action.accept(inputOfOptional); // default value if nothing matched
                    matchOptional = false;
                    rightSplit = null;
                    return true;
                }
                matchOptional = false;
                rightSplit = null;
            }
        }

        if (left.tryAdvance(b -> {
            right = (PausableStream<ID, VALUE>) executor.visit(lj.getRight(), b);
            inputOfOptional = b;
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

        return new SpliteratorOptional<>(split, context, input, lj).register(optionals);
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

    @Override
    public Op pause () {
        Op pausedLeft = leftStream.pause();

        List<Op> pausedRights = optionals.values().stream().map(SpliteratorOptional::pauseRight).filter(Objects::nonNull).toList();
        Op pausedRight = pausedRights.isEmpty() ?
                lj.getRight() :
                pausedRights.stream().reduce(DONE, removeEmptyOfUnion);

        if (Pause2Continuation.isDone(pausedLeft) && Pause2Continuation.isDone(pausedRight)) {
            return Pause2Continuation.DONE; // ofc
        }

        if (Pause2Continuation.notExecuted(lj.getLeft(), pausedLeft) && Pause2Continuation.notExecuted(lj.getRight(), pausedRight)) {
            return input.joinWith(lj); // still need the input to save.
        }

        if (Pause2Continuation.isDone(pausedLeft) && Pause2Continuation.notExecuted(lj.getRight(), pausedRight)) {
            return Pause2Continuation.DONE; // the input did not make the right progress, so we stop
        }

        if (Pause2Continuation.isDone(pausedLeft) || Pause2Continuation.notExecuted(lj.getLeft(), pausedLeft)) {
            return matchOptional ? input.joinWith(pausedRight) : input.leftJoinWith(pausedRight);
        }

        if (Pause2Continuation.isDone(pausedRight) || Pause2Continuation.notExecuted(lj.getRight(), pausedRight)) {
            // input should be already included in left
            return OpCloningUtil.clone(lj, pausedLeft, lj.getRight());
        }

        if (matchOptional) {
            return OpUnion.create(pausedRight, // right alone contains all, as a join
                    OpCloningUtil.clone(lj, pausedLeft, lj.getRight())); // the rest remains a left join
        } else { // otherwise we continue as a left join everywhere
            if (Objects.nonNull(inputOfOptional)) {
                return OpUnion.create(inputOfOptional.leftJoinWith(pausedRight),
                        OpCloningUtil.clone(lj, pausedLeft, lj.getRight()));
            } else {
                return OpUnion.create(input.leftJoinWith(pausedRight),
                        OpCloningUtil.clone(lj, pausedLeft, lj.getRight()));
            }
        }
    }

    public Op pauseRight () {
        return Objects.isNull(right) ? null : this.right.pause();
    }
}
