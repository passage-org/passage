package fr.gdd.raw.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.factories.IBackendOptionalsFactory;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class RawOptional<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    private final Iterator<BackendBindings<ID, VALUE>> leftInput;
    private final Op optionalOp;
    private final ExecutionContext execCxt;
    private Iterator<BackendBindings<ID, VALUE>> currentOptionalResults;
    private BackendBindings<ID, VALUE> nextBinding;
    final Backend<ID, VALUE, ?> backend;
    final BackendCache<ID,VALUE> cache;

    public RawOptional(Iterator<BackendBindings<ID, VALUE>> leftInput, Op optionalOp, ExecutionContext execCxt) {
        this.leftInput = leftInput;
        this.optionalOp = optionalOp;
        this.execCxt = execCxt;
        this.currentOptionalResults = null;
        this.nextBinding = null;
        this.backend = execCxt.getContext().get(RawConstants.BACKEND);
        this.cache = execCxt.getContext().get(RawConstants.CACHE);
    }

    @Override
    public boolean hasNext() {
        if (nextBinding != null) {
            return true; // Already have a binding ready to return
        }

        while (leftInput.hasNext()) {
            BackendBindings<ID, VALUE> leftBinding = leftInput.next();

            // Evaluate the optional part using the current left binding
            BackendOpExecutor<ID, VALUE> executor = (BackendOpExecutor<ID, VALUE>) execCxt.getContext().get(BackendConstants.EXECUTOR);
            currentOptionalResults = executor.visit(optionalOp, new SingletonIterator<>(leftBinding));

            if (currentOptionalResults.hasNext()) {
                // Optional part matched, return the combined bindings
                nextBinding = currentOptionalResults.next();
                return true;
            } else {
                // Optional part did not match, return the left binding as-is
                nextBinding = leftBinding;
                return true;
            }
        }

        return false; // No more bindings
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        if (!hasNext()) {
            throw new IllegalStateException("No more elements");
        }

        BackendBindings<ID, VALUE> result = nextBinding;
        nextBinding = null; // Clear the cached binding

        return result;
    }
    /**
     * Helper class to wrap a single binding into an iterator.
     */
    private static class SingletonIterator<ID, VALUE> implements Iterator<BackendBindings<ID, VALUE>> {
        private final BackendBindings<ID, VALUE> binding;
        private boolean consumed = false;

        public SingletonIterator(BackendBindings<ID, VALUE> binding) {
            this.binding = binding;
        }

        @Override
        public boolean hasNext() {
            return !consumed;
        }

        @Override
        public BackendBindings<ID, VALUE> next() {
            if (consumed) {
                throw new IllegalStateException("No more elements");
            }
            consumed = true;
            return binding;
        }
    }
    public static <ID, VALUE> IBackendOptionalsFactory<ID, VALUE> factory() {
        return (context, leftInput, optionalOp) -> {
            ExecutionContext execCxt = context.getContext().get(BackendConstants.EXECUTOR);
            return new RawOptional<>(leftInput, optionalOp, execCxt);
        };
    }
}