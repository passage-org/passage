package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor2;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;

public class PausableStreamJoin<ID,VALUE> implements PausableStream<ID, VALUE> {

    final PassagePushExecutor2<ID,VALUE> executor;
    final OpJoin join;
    final PausableStream<ID,VALUE> left;
    ConcurrentHashMap<Double, PausableStream<ID,VALUE>> rights;

    public PausableStreamJoin(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpJoin join) {
        this.join = join;
        this.executor = (PassagePushExecutor2<ID, VALUE>) context.executor;
        this.left = this.executor.visit(join.getLeft(), input);
        this.rights = new ConcurrentHashMap<>();
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return left.stream().flatMap(b -> {
            var newRight = executor.visit(join.getRight(), b);
            rights.put(Math.random(), newRight);
            return newRight.stream();
        });
    }

    @Override
    public Op pause() {
        Op pausedLeft = left.pause();

        // TODO, do left once, and right all at once, then combine.
        if (isDone(pausedLeft) && rights.isEmpty()) { return DONE; }
        if (rights.isEmpty()) { return OpJoin.create(pausedLeft, join.getRight()); }

        // no state needed really, everything is in the returned value of these:
        Op pausedRight = rights.values().stream().map(PausableStream::pause).reduce(DONE, removeEmptyOfUnion);

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

}
