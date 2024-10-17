package fr.gdd.passage.volcano.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.factories.IBackendOptionalsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * Always returns the left results when they exist, plus the right results optionally.
 */
public class PassageOptional<ID,VALUE>  implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendOptionalsFactory<ID,VALUE> factory() {
        return (context, input, op) -> {
            BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            BackendSaver<ID,VALUE,?> saver = context.getContext().get(PassageConstants.SAVER);
            Iterator<BackendBindings<ID,VALUE>> optionals = new PassageOptional<>(context, executor, op, input);
            saver.register(op, optionals);
            return optionals;
        };
    }

    final Op2 op;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final ExecutionContext context;
    final BackendOpExecutor<ID,VALUE> executor;

    BackendBindings<ID,VALUE> inputBinding;
    Iterator<BackendBindings<ID,VALUE>> mandatory = Iter.empty();
    BackendBindings<ID,VALUE> mandatoryBinding;
    Iterator<BackendBindings<ID,VALUE>> optional = Iter.empty();
    Boolean noOptionalPart = true; // saving the fact that the optional exist or not
    BackendBindings<ID,VALUE> optionalBinding;

    public PassageOptional(ExecutionContext context, BackendOpExecutor<ID, VALUE> executor, Op2 op, Iterator<BackendBindings<ID,VALUE>> input) {
        this.op = op;
        this.input = input;
        this.context = context;
        this.executor = executor;

        Pause2Next<ID,VALUE> saver = context.getContext().get(PassageConstants.SAVER);
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
        if (optional.hasNext() || mandatory.hasNext()) {
            return true;
        }

        if (!input.hasNext()) { // we need input at this point if optional and mandatory failed
            return false;
        }

        // otherwise, need to create iterator over the mandatory part
        while (!mandatory.hasNext() && input.hasNext()) {
            inputBinding = input.next();

            mandatory = ReturningArgsOpVisitorRouter.visit(executor, op.getLeft(), Iter.of(inputBinding));
            mandatoryBinding = null;
        }

        return mandatory.hasNext();

//        // if the mandatory part exists, no need to iterate, since we will return it;
//        // but we do not need to next each time…
//        if (Objects.isNull(mandatoryBinding)) {
//            mandatoryBinding = mandatory.next();
//        };
//
//        optional = ReturningArgsOpVisitorRouter.visit(executor, op.getRight(), Iter.of(mandatoryBinding));
//        noOptionalPart = !optional.hasNext(); // make sure all hasNext are called in hasNext
//        if (!noOptionalPart) {
//            mandatoryBinding = null;
//        }
//        return true;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        // At this point, we know that at least mandatory exists.
        if (Objects.isNull(mandatoryBinding)) {
            mandatoryBinding = mandatory.next();
            optional = ReturningArgsOpVisitorRouter.visit(executor, op.getRight(), Iter.of(mandatoryBinding));
            optionalBinding = null;
            noOptionalPart = true;
        }

        // but we check optional first, since it's priority 1
        if (optional.hasNext()) {
            noOptionalPart = false;
            optionalBinding = optional.next();
            return optionalBinding;
        }

        // otherwise, we check mandatory, but it should not produce if optional produced
        // even in previous calls to next.
        if (Objects.isNull(optionalBinding)) {
            BackendBindings<ID, VALUE> consumed = mandatoryBinding;
            mandatoryBinding = null;
            return consumed;
        } else {
            // optional has been fully consumed before, mandatory next needed
            mandatoryBinding = null;
            return this.next();
        }
    }

    public Op pause(Op preemptedRight) {
        if (Objects.isNull(mandatoryBinding) && (Objects.isNull(optionalBinding) || !optional.hasNext())) return null; // TODO probably something to do with this condition

        Set<Var> mandatoryVars = mandatoryBinding.vars();
        OpSequence seq = OpSequence.create();
        for (Var v : mandatoryVars) {
            seq.add(OpExtend.extend(OpTable.unit(), v, ExprUtils.parse(mandatoryBinding.get(v).getString())));
        }

        Set<Var> optionalVars = OpVars.visibleVars(op.getRight());
        optionalVars.removeAll(mandatoryVars);
        if (optionalVars.isEmpty()) {
            // if there are no variables, it means that the optional part
            // ended up being a simple check. Either passed or failed, the
            // mandatory part only remains. The optional part is removed.
            return seq;
        }

        if (preemptedRight instanceof OpProject preemptedProject) {
            // instead of creating project of project of project, we
            // take advantage of the already present one. The subop should
            // not return the binding of the mandatory part, so we filter
            // them out.
            optionalVars = new HashSet<>(preemptedProject.getVars());
            optionalVars.removeAll(mandatoryVars);
            Op subop = new OpProject(preemptedProject.getSubOp(), optionalVars.stream().toList());
            return OpLeftJoin.create(seq, subop, ExprList.emptyList);
        }

        Op subop = new OpProject(preemptedRight, optionalVars.stream().toList());

        return OpLeftJoin.create(seq, subop, ExprList.emptyList);
    }
}
