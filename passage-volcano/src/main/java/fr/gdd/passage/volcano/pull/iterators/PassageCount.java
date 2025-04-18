package fr.gdd.passage.volcano.pull.iterators;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.engines.BackendPullExecutor;
import fr.gdd.passage.commons.factories.IBackendCountsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import fr.gdd.passage.commons.iterators.BackendIteratorOverInput;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.pull.Pause2Next;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
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
        return (context, input, agg) -> new BackendIteratorOverInput<>(context, input, agg, PassageCount::new);
    }

    final BackendPullExecutor<ID,VALUE> executor;
    final Backend<ID,VALUE> backend;
    final OpGroup opCount;
    final BackendBindings<ID,VALUE> input;
    final BackendBindings<ID,VALUE> keys;
    final PassageExecutionContext<ID,VALUE> context;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;

    Pair<Var, BackendAccumulator<ID,VALUE>> var2accumulator = null; // TODO map of Var -> accumulator
    long produced = 0L;

    public PassageCount(ExecutionContext context, BackendBindings<ID,VALUE> input, OpGroup opCount){
        this.context = (PassageExecutionContext<ID, VALUE>) context;
        this.backend = this.context.backend;
        this.executor = context.getContext().get(BackendConstants.EXECUTOR); // TODO put it in context
        this.opCount = opCount;
        this.input = input;


        keys = getKeyBinding(opCount.getGroupVars().getVars(), input);
        if (keys.variables().stream().anyMatch(v -> Objects.isNull(keys.getBinding(v)))) {
            // TODO if variables that are in keys are not in the input,
            //      then they should be enumerated as a DISTINCT iterator.
            //      Continuation queries on DISTINCT might be feasible only
            //      under some conditions.
            throw new UnsupportedOperationException("Unbounded GROUP BY keys in COUNT are not supported yet.");
        }

        for (ExprAggregator agg : opCount.getAggregators() ) {
            BackendAccumulator<ID,VALUE> passageCount = switch (agg.getAggregator()) {
                case AggCount ignored -> new PassageCountAccumulator<>(context, opCount);
                case AggCountVar ignored -> new PassageCountAccumulator<>(context, opCount);
                default -> throw new UnsupportedOperationException("The aggregator is not supported.");
            };
            Var v = agg.getVar();
            var2accumulator = new ImmutablePair<>(v, passageCount);
        }

        // automatically join with the input, but only on keys
        wrapped = executor.visit(this.opCount.getSubOp(), Iter.of(keys));

        Pause2Next<ID,VALUE> saver = executor.context.getContext().get(PassageConstants.SAVER);
        saver.register(opCount, this);
    }

    @Override
    public boolean hasNext() {
        // if the wrapped does not have a next, it means that
        // the aggregate should not even exist.
        if (opCount.getGroupVars().isEmpty()) {
            return produced < 1;
        }
        return wrapped.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        while (wrapped.hasNext()) {
            BackendBindings<ID,VALUE> bindings = wrapped.next();
            var2accumulator.getRight().accumulate(bindings, executor.context);
        }
        produced += 1;
        return createBinding(var2accumulator);
    }

    /**
     * If paused during the execution, it should return an expression
     * using the count. For instance `SELECT ((COUNT(*) + 12) AS ?count)…`.
     */
    public Op save(OpExtend parent, Op subop) {
        // It should save the current count value processed until there.
        BackendBindings<ID,VALUE> export = createBinding(var2accumulator);

        OpGroup clonedGB = OpCloningUtil.clone(opCount, subop);
        OpExtend cloned = OpCloningUtil.clone(parent, clonedGB);

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
            // binding = binding.substring(1, binding.length()-1); // ugly af

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

        return OpJoin.create(input.asBindAs(), cloned); // add the environment mapping
    }


    /* ************************************** UTILITIES ************************************** */

    private static <ID,VALUE> BackendBindings<ID,VALUE> getKeyBinding(List<Var> vars, BackendBindings<ID,VALUE> binding) {
        BackendBindings<ID,VALUE> keyBinding = new BackendBindings<>();
        vars.forEach(v -> keyBinding.put(v, binding.getBinding(v)));
        return keyBinding;
    }

    private BackendBindings<ID,VALUE> createBinding(Pair<Var, BackendAccumulator<ID,VALUE>> var2acc) {
        BackendBindings<ID,VALUE> newBinding = new BackendBindings<>();
        newBinding.put(var2acc.getLeft(), new BackendBindings.IdValueBackend<ID,VALUE>()
                .setValue(var2acc.getRight().getValue()));
        return newBinding;
    }

}
