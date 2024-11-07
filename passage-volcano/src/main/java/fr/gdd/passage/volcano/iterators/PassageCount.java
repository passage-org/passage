package fr.gdd.passage.volcano.iterators;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.factories.IBackendCountsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.accumulators.PassageAccCount;
import fr.gdd.passage.volcano.accumulators.PassageAccumulator;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * An aggregator function dedicated to `COUNT` clauses.
 * *
 * When paused, it should save the current `count` value in a
 * `BIND` so the result of the continuation query takes it into account
 * as a simple summation.
 */
public class PassageCount<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendCountsFactory<ID,VALUE> factory() {
        return (context, input, group) -> {
            // BackendSaver<ID,VALUE,?> saver = context.getContext().get(PassageConstants.SAVER);
            BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            Iterator<BackendBindings<ID,VALUE>> it = new PassageCount.BackendCountsFactory<>(context, input, group, executor);
            // saver.register(distinct, distincts);
            return it;
        };
    }

    /* ********************** FACTORY OF ITERATOR PER INPUT ******************* */

    public static class BackendCountsFactory<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

        final ExecutionContext context;
        final Iterator<BackendBindings<ID,VALUE>> input;
        final OpGroup op;
        final BackendOpExecutor<ID,VALUE> executor;
        Iterator<BackendBindings<ID,VALUE>> wrapped;

        public BackendCountsFactory (ExecutionContext context, Iterator<BackendBindings<ID,VALUE>> input, OpGroup op,
                                     BackendOpExecutor<ID,VALUE> executor) {
            this.context = context;
            this.input = input;
            this.op = op;
            this.executor = executor;
        }

        @Override
        public boolean hasNext() {
            if ((Objects.isNull(wrapped) || !wrapped.hasNext()) && !input.hasNext()) return false;

            if (Objects.nonNull(wrapped) && !wrapped.hasNext()) {
                wrapped = null;
            }

            while (Objects.isNull(wrapped) && input.hasNext()) {
                BackendBindings<ID,VALUE> bindings = input.next();
                wrapped = new PassageCount<>(context, executor, op, bindings);
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


    /* ************************** ACTUAL ITERATOR ************************** */

    final BackendOpExecutor<ID,VALUE> executor;
    final OpGroup op;
    final BackendBindings<ID,VALUE> input;
    final ExecutionContext context;

    Pair<Var, BackendAccumulator<ID,VALUE>> var2accumulator = null;
    long produced = 0L;

    public PassageCount(ExecutionContext context, BackendOpExecutor<ID, VALUE> executor, OpGroup op, BackendBindings<ID,VALUE> input){
        this.executor = executor;
        this.op = op;
        this.input = input;
        this.context = context;

        for (ExprAggregator agg : op.getAggregators() ) {
            BackendAccumulator<ID,VALUE> passageCount = switch (agg.getAggregator()) {
                case AggCount ignored -> new PassageAccCount<>(context, op.getSubOp());
                default -> throw new UnsupportedOperationException("The aggregator is not supported yet.");
            };
            Var v = agg.getVar();
            var2accumulator = new ImmutablePair<>(v, passageCount);
        }

        Pause2Next<ID,VALUE> saver = executor.context.getContext().get(PassageConstants.SAVER);
        saver.register(op, this);

    }

    @Override
    public boolean hasNext() {
        return produced < 1; // TODO fix that
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        produced += 1;
        Iterator<BackendBindings<ID,VALUE>> subop = ReturningArgsOpVisitorRouter.visit(executor, op.getSubOp(), Iter.of(input));

        while (subop.hasNext()) {
            BackendBindings<ID,VALUE> bindings = subop.next();
            // BackendBindings<ID,VALUE> keyBinding = getKeyBinding(op.getGroupVars().getVars(), bindings);

            var2accumulator.getRight().accumulate(bindings, executor.context);
        }

        return createBinding(var2accumulator);
    }


    public OpExtend save(OpExtend parent, Op subop) {
        BackendBindings<ID,VALUE> export = createBinding(var2accumulator);

        OpGroup clonedGB = OpCloningUtil.clone(op, subop);
        OpExtend cloned = OpCloningUtil.clone(parent, clonedGB);

//        for (Var v : inputBinding.vars()) {
//            clonedGB.getGroupVars().add(v);
//        }

        VarExprList exprList = parent.getVarExprList();
        for (int i = 0; i < exprList.size(); ++i) {
            Var varFullName = exprList.getVars().get(i);
            Var varRenamed = null;
            if (exprList.getExpr(varFullName) instanceof ExprVar exprVar) {
                varRenamed = exprVar.asVar();
            } else if (exprList.getExpr(varFullName) instanceof E_Add add) {
                varRenamed = add.getArg1().asVar();
            }

            String binding = export.getBinding(varRenamed).getString(); // substr because it has ""
            binding = binding.substring(1, binding.length()-1); // ugly af

            NodeValueInteger oldValue = new NodeValueInteger(0);
            if (exprList.getExpr(varFullName) instanceof E_Add add) {
                oldValue = (NodeValueInteger) add.getArg2();
            }

            NodeValue newValue = ExprUtils.eval(new E_Add(oldValue, NodeValue.parse(binding)));

            Expr newExpr = newValue.equals(new NodeValueInteger((0))) ? // 0 is default, so we can remove it when it is
                    new ExprVar(varRenamed) :
                    new E_Add(new ExprVar(varRenamed), newValue); // ugly af
            cloned.getVarExprList().remove(varFullName);
            cloned.getVarExprList().add(varFullName, newExpr);
        }

        return cloned;
    }


    /* ************************************************************************* */

    private static <ID,VALUE> BackendBindings<ID,VALUE> getKeyBinding(List<Var> vars, BackendBindings<ID,VALUE> binding) {
        BackendBindings<ID,VALUE> keyBinding = new BackendBindings<>();
        vars.forEach(v -> keyBinding.put(v, binding.getBinding(v)));
        return keyBinding;
    }

    private BackendBindings<ID,VALUE> createBinding(Pair<Var, BackendAccumulator<ID,VALUE>> var2acc) {
        BackendBindings<ID,VALUE> newBinding = new BackendBindings<>();
        newBinding.put(var2acc.getLeft(), new BackendBindings.IdValueBackend<ID,VALUE>()
                .setBackend(context.getContext().get(BackendConstants.BACKEND))
                .setValue(var2acc.getRight().getValue()));
        return newBinding;
    }

}
