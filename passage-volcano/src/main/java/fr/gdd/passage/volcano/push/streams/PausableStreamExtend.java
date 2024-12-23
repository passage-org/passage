package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.querypatterns.IsGroupByQuery;
import fr.gdd.passage.volcano.transforms.FactorizeExtends;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;

import java.util.Map;
import java.util.stream.Stream;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;

public class PausableStreamExtend<ID,VALUE> implements PausableStream<ID, VALUE> {

    final PausableStream<ID,VALUE> wrapped;
    final PassagePushExecutor<ID, VALUE> executor;
    final OpExtend extend;
    final PassageExecutionContext<ID,VALUE> context;

    public PausableStreamExtend(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpExtend extend) {
        this.executor = (PassagePushExecutor<ID, VALUE>) context.executor;
        this.wrapped = executor.visit(extend.getSubOp(), input);
        this.extend = extend;
        this.context = context;
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return wrapped.stream().map(i -> {
            BackendBindings<ID, VALUE> b = context.bindingsFactory.get().setParent(i);

            for (Map.Entry<Var, Expr> varAndExpr : extend.getVarExprList().getExprs().entrySet()) {
                // TODO cache the thing when simple
                if (varAndExpr.getValue() instanceof NodeValueNode toCache) {
                    context.bindingsFactory.cache.register(toCache.asNode(), null);
                }
                b.put(varAndExpr.getKey(), new BackendBindings.IdValueBackend<ID, VALUE>()
                        .setBackend(context.backend)
                        .setString(NodeFmtLib.strNT(varAndExpr.getValue().eval(i, context).asNode())));
            }

            return b;
        });
    }

    @Override
    public Op pause() {
        Op subop = wrapped.pause();
        if (notExecuted(extend.getSubOp(), subop)) return extend;
        if (isDone(subop)) return DONE;

        if (new IsGroupByQuery().visit(extend)) {
            OpExtend op = (subop instanceof OpExtend subExtend) ?
                    FactorizeExtends.factorize(extend, subExtend):
                    OpCloningUtil.clone(extend, subop);

            if (op.getSubOp() instanceof OpJoin join) {
                Op inputToPushUp = join.getLeft();
                Op groupByToKeep = join.getRight();

                return OpJoin.create(inputToPushUp, OpCloningUtil.clone(op, groupByToKeep));
            }
            return op;
        }

        return OpCloningUtil.clone(extend, subop);
    }
}
