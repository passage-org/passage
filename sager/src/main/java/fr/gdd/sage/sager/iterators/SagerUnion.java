package fr.gdd.sage.sager.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;

import java.util.Iterator;
import java.util.Objects;

public class SagerUnion<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final Iterator<BackendBindings<ID, VALUE>> input;
    final Op left;
    final Op right;
    final SagerOpExecutor<ID, VALUE> executor;

    BackendBindings<ID, VALUE> current = null;
    Integer currentOp = -1; // -1 not init, 0 left, 1 right
    Iterator<BackendBindings<ID, VALUE>> currentIt;

    public SagerUnion(SagerOpExecutor<ID, VALUE> executor, Iterator<BackendBindings<ID, VALUE>> input, Op left, Op right) {
        this.left = left;
        this.right = right;
        this.input = input;
        this.executor = executor;
    }

    @Override
    public boolean hasNext() {
        if (Objects.isNull(current) && input.hasNext()) {
            current = input.next();
        }
        if (Objects.isNull(current)) { return false; } // recursive call might not trigger

        while (Objects.isNull(currentIt) || !currentIt.hasNext()) {
            if (currentOp < 0) {
                currentOp = 0;
                currentIt = ReturningArgsOpVisitorRouter.visit(executor, left, Iter.of(current));
            }

            if (currentOp == 0 && !currentIt.hasNext()) {
                currentOp = 1;
                currentIt = ReturningArgsOpVisitorRouter.visit(executor, right, Iter.of(current));
            }

            if (currentOp == 1 && !currentIt.hasNext()) {
                currentOp = -1;
                current = null;
                if (!input.hasNext()) {
                    return false;
                } else {
                    return this.hasNext();
                }
            }
        }

        return true; // by the loop, currentIt.hasNext() is always true at this point
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return currentIt.next();
    }

    public boolean onLeft () {
        return currentOp == 0;
    }

    public boolean onRight () {
        return currentOp == 1;
    }
}
