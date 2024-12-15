package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.push.Pause2Continuation;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// TODO TODO TODO
public class PausableStreamOptional<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final Stream<BackendBindings<ID,VALUE>> wrapped;
    final BackendBindings<ID,VALUE> input;
    final OpLeftJoin lj;
    final PassagePushExecutor<ID,VALUE> executor;

    final AtomicBoolean matchOptionalClause = new AtomicBoolean(false);
    AtomicReference<BackendBindings<ID,VALUE>> inputOfOptional = new AtomicReference<>();

    public PausableStreamOptional(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpLeftJoin lj) {
        this.context = context;
        this.executor = (PassagePushExecutor<ID, VALUE>) this.context.executor;
        this.lj = lj;
        this.input = input;

        this.wrapped = executor.visit(lj.getLeft(), input).stream()
                .flatMap(m -> {
                    inputOfOptional.set(m);
                    // iterator to call hasNext without consuming the stream nor to have to create a new one.
                    Iterator<BackendBindings<ID,VALUE>> asIterator = executor.visit(lj.getRight(), m).stream().iterator();
                    if ((asIterator.hasNext())) {
                        matchOptionalClause.set(true);
                        // convert back to stream if it matches the optional
                        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(asIterator, 0),
                                this.context.maxParallelism > 1);
                    } else {
                        return Stream.of(m); // default value
                    }});
    }

    public Stream<BackendBindings<ID,VALUE>> stream() {
        return this.wrapped;
    }

    @Override
    public Op pause() {
        Op left = null; // TODO new Pause2Continuation<>(context.op2its).visit(lj.getLeft());
        Op right = null; // TODO new Pause2Continuation<>(context.op2its).visit(lj.getRight());

        if (Pause2Continuation.isDone(left) && Pause2Continuation.isDone(right)) {
            return Pause2Continuation.DONE; // ofc
        }

        if (Pause2Continuation.notExecuted(lj.getLeft(), left) && Pause2Continuation.notExecuted(lj.getRight(), right)) {
            return input.joinWith(lj); // still need the input to save.
        }

        if (Pause2Continuation.isDone(left) && Pause2Continuation.notExecuted(lj.getRight(), right)) {
            return Pause2Continuation.DONE; // the input did not make the right progress, so we stop
        }

        if (Pause2Continuation.isDone(left) || Pause2Continuation.notExecuted(lj.getLeft(), left)) {
            return matchOptionalClause.get() ? input.joinWith(right) : input.leftJoinWith(right);
        }

        if (Pause2Continuation.isDone(right) || Pause2Continuation.notExecuted(lj.getRight(), right)) {
            // input should be already included in left
            return OpCloningUtil.clone(lj, left, lj.getRight());
        }

        if (matchOptionalClause.get()) {
            return OpUnion.create(right, // right alone contains all, as a join
                    OpCloningUtil.clone(lj, left, lj.getRight())); // the rest remains a left join
        } else { // otherwise we continue as a left join everywhere
            return OpUnion.create(input.leftJoinWith(right),
                    OpCloningUtil.clone(lj, left, lj.getRight()));
        }
    }
}
