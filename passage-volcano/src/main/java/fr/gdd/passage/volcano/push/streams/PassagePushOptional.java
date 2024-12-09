package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.push.Pause2ContinuationQuery;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PassagePushOptional<ID,VALUE> extends PausableSpliterator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;
    final Stream<BackendBindings<ID,VALUE>> wrapped;
    final BackendBindings<ID,VALUE> input;
    final OpLeftJoin lj;
    final PassagePushExecutor<ID,VALUE> executor;

    final AtomicBoolean matchOptionalClause = new AtomicBoolean(false);
    AtomicReference<BackendBindings<ID,VALUE>> inputOfOptional = new AtomicReference<>();

    public PassagePushOptional(ExecutionContext context, BackendBindings<ID,VALUE> input, OpLeftJoin lj) {
        super((PassageExecutionContext<ID, VALUE>) context, lj);
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.executor = (PassagePushExecutor<ID, VALUE>) this.context.executor;
        this.lj = lj;
        this.input = input;

        this.wrapped = executor.visit(lj.getLeft(), input)
                .flatMap(m -> {
                    inputOfOptional.set(m);
                    // iterator to call hasNext without consuming the stream nor to have to create a new one.
                    Iterator<BackendBindings<ID,VALUE>> asIterator = executor.visit(lj.getRight(), m).iterator();
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
        Op left = new Pause2ContinuationQuery<>(context.op2its).visit(lj.getLeft());
        Op right = new Pause2ContinuationQuery<>(context.op2its).visit(lj.getRight());

        if (Objects.isNull(left) && Objects.isNull(right)) {
            return null;
        } else if (Objects.isNull(left)) { // only right is not null, but might not be found
            if (matchOptionalClause.get()) {
                return right; // everything is set in the right part
            } else {
                return OpCloningUtil.clone(lj, inputOfOptional.get().toOp(), right);
            }
        } else if (Objects.isNull(right)) { // only left is not null
            return OpCloningUtil.clone(lj, left, lj.getRight());
        } else { // both sides are set
            return OpUnion.create(OpCloningUtil.clone(lj,left, lj.getRight()), // rest of left
                    (matchOptionalClause.get()) ?
                            right: // everything is set in the right part
                            OpCloningUtil.clone(lj, inputOfOptional.get().toOp(), right));
        }
    }
}
