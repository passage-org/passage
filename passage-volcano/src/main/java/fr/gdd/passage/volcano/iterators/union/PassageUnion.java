package fr.gdd.passage.volcano.iterators.union;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.passage.commons.factories.IBackendUnionsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.iterators.BackendIteratorOverInput;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.iterators.PausableIterator;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/**
 * Basic union iterator that keeps track of the branch it executes.
 */
public class PassageUnion<ID, VALUE> extends PausableIterator<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendUnionsFactory<ID,VALUE> factory() {
        return (context, input, union) -> new BackendIteratorOverInput<>(context, input, union, PassageUnion::new);
    }

    final BackendBindings<ID, VALUE> input;
    final Op left;
    final Op right;
    final BackendOpExecutor<ID, VALUE> executor;
    final PassageExecutionContext<ID,VALUE> context;

    BackendBindings<ID, VALUE> current = null;
    Integer currentOp = -1; // -1 not init, 0 left, 1 right
    Iterator<BackendBindings<ID, VALUE>> wrapped;
    boolean consumed = true;

    public PassageUnion(ExecutionContext context, BackendBindings<ID, VALUE> input, OpUnion union) {
        super((PassageExecutionContext<ID, VALUE>) context, union);
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.executor = this.context.getContext().get(BackendConstants.EXECUTOR);
        this.left = union.getLeft();
        this.right = union.getRight();
        this.input = input;
    }

    @Override
    public boolean hasNext() {
        if (!consumed) {return true;}
        if (currentOp == 2) {return false;}
        while (Objects.isNull(wrapped) || !wrapped.hasNext()) {
            if (currentOp < 0) {
                currentOp = 0;
                wrapped = executor.visit(left, Iter.of(input));
            }

            if (currentOp == 0 && (Objects.isNull(wrapped) || !wrapped.hasNext())) {
                currentOp = 1;
                wrapped = executor.visit(right, Iter.of(input));
            }

            if (currentOp == 1 && (Objects.isNull(wrapped) || !wrapped.hasNext())) {
                currentOp = 2;
                current = null;
                consumed = true;
                return false;
            }
        }
        consumed = false;
        return true; // by the loop, currentIt.hasNext() is always true at this point
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        consumed = true;
        return wrapped.next();
    }

    public boolean onLeft () {
        return currentOp == 0;
    }

    public boolean onRight () {
        return currentOp == 1;
    }

    @Override
    public Op pause() {
        if (currentOp>1) {return null;} // done

        // environment mappings `input` should be
        // saved within the downstream operators

        if (this.onLeft()) {  // preempt the left, copy the right for later
            Op pausedLeft = context.saver.visit(this.left);
            return FlattenUnflatten.unflattenUnion(Arrays.asList(pausedLeft, this.right));
        } else { // on right: remove left part of union
            return context.saver.visit(this.right);
        }
    }
}
