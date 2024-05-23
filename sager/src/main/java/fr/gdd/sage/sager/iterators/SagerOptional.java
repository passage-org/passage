package fr.gdd.sage.sager.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

/**
 * Always returns the left results when they exist, plus the right results optionally.
 */
public class SagerOptional<ID,VALUE>  implements Iterator<BackendBindings<ID, VALUE>> {

    final Op2 op;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final ExecutionContext context;
    final SagerOpExecutor<ID,VALUE> executor;

    BackendBindings<ID,VALUE> inputBinding;
    Iterator<BackendBindings<ID,VALUE>> mandatory = Iter.empty();
    BackendBindings<ID,VALUE> mandatoryBinding;
    Iterator<BackendBindings<ID,VALUE>> optional = Iter.empty();
    Boolean noOptionalPart = true; // saving the fact that the optional exist or not

    public SagerOptional(SagerOpExecutor<ID, VALUE> executor, Op2 op, Iterator<BackendBindings<ID,VALUE>> input, ExecutionContext context) {
        this.op = op;
        this.input = input;
        this.context = context;
        this.executor = executor;

        Save2SPARQL<ID,VALUE> saver = context.getContext().get(SagerConstants.SAVER);
        saver.register(op, this);
    }

    public boolean hasOptionalPart() {
        return !noOptionalPart;
    }

    public boolean DoesNotHaveOptionalPart() {
        return noOptionalPart;
    }

    @Override
    public boolean hasNext() {
        // optional part already exists and need to be iterated over
        if (optional.hasNext()) {
            return true;
        }

        if (!mandatory.hasNext() && !input.hasNext()) {
            return false;
        }

        // otherwise, need to create iterator over the mandatory part
        while (!mandatory.hasNext() && input.hasNext()) {
            inputBinding = input.next();

            mandatory = ReturningArgsOpVisitorRouter.visit(executor, op.getLeft(), Iter.of(inputBinding));
        }
        if (!mandatory.hasNext()) { // after iterating over all elements, might not exists, so we stop there.
            return false;
        }

        // if the mandatory part exists, no need to iterate, since we will return it;
        mandatoryBinding = mandatory.next();
        optional = ReturningArgsOpVisitorRouter.visit(executor, op.getRight(), Iter.of(mandatoryBinding));
        noOptionalPart = !optional.hasNext(); // make sure all hasNext are called in hasNext
        return true;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return noOptionalPart ?
                mandatoryBinding :
                optional.next();
    }
}
