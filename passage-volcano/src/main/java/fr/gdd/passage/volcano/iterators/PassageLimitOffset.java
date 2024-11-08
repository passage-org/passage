package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.factories.IBackendLimitOffsetFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageSubOpExecutor;
import fr.gdd.passage.volcano.resume.CanBeSkipped;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Does not actually process any LIMIT OFFSET. It checks if it's a valid
 * subquery (i.e., one that can be paused/resumed) then it executes it.
 * Preemption mostly comes from this: the ability to start over from an OFFSET efficiently.
 * When we find a pattern like SELECT * WHERE {?s ?p ?o} OFFSET X, the engine know that
 * it must skip X elements of the iterator. But the pattern must be accurate: a single
 * triple pattern.
 */
public class PassageLimitOffset<ID,VALUE> implements IBackendLimitOffsetFactory<ID,VALUE> {

    @Override
    public Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, OpSlice slice) {
        BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
        return new CompatibilityCheckIteratorFactory<>(executor, input, slice); // so they should be checked
    }

    /* *************************** FACTORY OF ITERATOR PER INPUT ************************* */
    // This should be avoided as much as possible since the compatibility check is inefficient.
    // However, with some sub-queries, it cannot be avoided.

    public static class CompatibilityCheckIteratorFactory<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

        final BackendOpExecutor<ID,VALUE> executor;
        final Iterator<BackendBindings<ID,VALUE>> input;
        final OpSlice subquery;
        Iterator<BackendBindings<ID,VALUE>> wrapped;

        public CompatibilityCheckIteratorFactory (BackendOpExecutor<ID,VALUE> executor,
                                                  Iterator<BackendBindings<ID,VALUE>> input,
                                                  OpSlice subquery) {
            this.executor = executor;
            this.input = input;
            this.subquery = subquery;
        }

        @Override
        public boolean hasNext() {
            if ((Objects.isNull(wrapped) || !wrapped.hasNext()) && !input.hasNext()) return false;

            if (Objects.nonNull(wrapped) && !wrapped.hasNext()) {
                wrapped = null;
            }

            while (Objects.isNull(wrapped) && input.hasNext()) {
                BackendBindings<ID,VALUE> bindings = input.next();
                wrapped = new CompatibilityCheckIterator<>(executor, bindings, subquery);
                if (!wrapped.hasNext()) {
                    wrapped = null;
                }
            }

            return !Objects.isNull(wrapped);
        }

        @Override
        public BackendBindings<ID, VALUE> next() {
            return wrapped.next();
        }
    }

    /* ******************************* ACTUAL ITERATOR ****************************** */

    public static class CompatibilityCheckIterator<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

        final BackendOpExecutor<ID,VALUE> executor;
        final BackendBindings<ID,VALUE> input;
        final Iterator<BackendBindings<ID,VALUE>> wrapped;
        final OpSlice subquery;
        BackendBindings<ID,VALUE> produced;

        public CompatibilityCheckIterator(BackendOpExecutor<ID,VALUE> executor, BackendBindings<ID,VALUE> input, OpSlice subquery) {
            this.executor = executor;
            this.input = input;

            Boolean canSkip = new CanBeSkipped().visit((Op) subquery);

            PassageExecutionContext<ID,VALUE> subContext = ((PassageExecutionContext<ID, VALUE>) executor.context).clone();
            subContext.setLimit(subquery.getLength());
            subContext.setOffset(subquery.getStart());
            subContext.setQuery(subquery.getSubOp());
            if (canSkip) { // for simple sub-query comprising a single TP/QP, efficient skip is allowed.
                this.wrapped = new PassageSubOpExecutor<ID,VALUE>(subContext).visit(subquery.getSubOp(), Iter.of(new BackendBindings<>()));
            } else {
                // otherwise, resort to a slow enumeration of results until OFFSET is reached.
                this.wrapped = new PassageLimit<>(subContext, subquery);
            }
            BackendSaver<ID, VALUE, ?> saver = executor.context.getContext().get(PassageConstants.SAVER);
            saver.register(subquery, this);
            // this.wrapped = executor.visit(subquery, Iter.of(new BackendBindings<>()));
            this.subquery = subquery;
        }

        @Override
        public boolean hasNext() {
            if (Objects.nonNull(produced)) { return true; }
            boolean compatible = false;
            while (!compatible && wrapped.hasNext()) {
                produced = wrapped.next();
                compatible = produced.isCompatible(input);
            }

            return compatible;
        }

        @Override
        public BackendBindings<ID, VALUE> next() {
            // produced has its own chain of parents, so we cannot set parent
            // easily, so we copy for now.
            BackendBindings<ID,VALUE> toReturn = new BackendBindings<>(produced, produced.variables().stream().toList());
            toReturn.setParent(input);
            produced = null; // consumed
            return toReturn;
        }

        public Op pause(Op inside) {
            if (!wrapped.hasNext()) {
                return null;
            }

            Set<Var> vars = input.variables();
            OpSequence seq = OpSequence.create();
            for (Var v : vars) {
                seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(input.getBinding(v).getString())));
            }
            if (wrapped instanceof PassageLimit<ID,VALUE> complexQuery) {
                seq.add(complexQuery.pause(inside));
            } else if (wrapped instanceof PassageScanFactory simpleQuery) { // TODO unuglify
                seq.add(inside);
            }

            Op seqOrSingle = seq.size() > 1 ? seq : seq.get(0);
            return seqOrSingle;
        }
    }

}
