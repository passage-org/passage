package fr.gdd.passage.volcano.iterators;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.volcano.accumulators.PassageAccCount;
import fr.gdd.passage.volcano.accumulators.PassageAccumulator;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageOpExecutor;
import fr.gdd.passage.volcano.pause.Pause2SPARQL;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.List;

public class PassageAgg<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final PassageOpExecutor<ID,VALUE> executor;
    final OpGroup op;
    final Iterator<BackendBindings<ID,VALUE>> input;

    BackendBindings<ID,VALUE> inputBinding;
    Pair<Var, PassageAccumulator<ID,VALUE>> var2accumulator = null;

    public PassageAgg(PassageOpExecutor<ID, VALUE> executor, OpGroup op, Iterator<BackendBindings<ID,VALUE>> input){
        this.executor = executor;
        this.op = op;
        this.input = input;

        for (ExprAggregator agg : op.getAggregators() ) {
            PassageAccumulator<ID,VALUE> sagerX = switch (agg.getAggregator()) {
                case AggCount ac -> new PassageAccCount<>(executor.getExecutionContext(), op.getSubOp());
                default -> throw new UnsupportedOperationException("The aggregator is not supported yet.");
            };
            Var v = agg.getVar();
            var2accumulator = new ImmutablePair<>(v, sagerX);
        }

        Pause2SPARQL<ID,VALUE> saver = executor.getExecutionContext().getContext().get(PassageConstants.SAVER);
        saver.register(op, this);

    }

    @Override
    public boolean hasNext() {
        return this.input.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        inputBinding = input.next();
        Iterator<BackendBindings<ID,VALUE>> subop = ReturningArgsOpVisitorRouter.visit(executor, op.getSubOp(), Iter.of(inputBinding));

        while (subop.hasNext()) {
            BackendBindings<ID,VALUE> bindings = subop.next();
            // BackendBindings<ID,VALUE> keyBinding = getKeyBinding(op.getGroupVars().getVars(), bindings);

            var2accumulator.getRight().accumulate(bindings, executor.getExecutionContext());
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

            String binding = export.get(varRenamed).getString(); // substr because it has ""
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
        vars.forEach(v -> keyBinding.put(v, binding.get(v)));
        return keyBinding;
    }

    private BackendBindings<ID,VALUE> createBinding(Pair<Var, PassageAccumulator<ID,VALUE>> var2acc) {
        BackendBindings<ID,VALUE> newBinding = new BackendBindings<>();
        newBinding.put(var2acc.getLeft(), new BackendBindings.IdValueBackend<ID,VALUE>()
                .setBackend(executor.getBackend())
                .setValue(var2acc.getRight().getValue()));
        return newBinding;
    }

}
