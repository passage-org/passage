package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.core.Var;

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
        return wrapped.stream().parallel().map(i -> {
            BackendBindings<ID, VALUE> b = new BackendBindings<ID, VALUE>().setParent(i);
            for (Var v : extend.getVarExprList().getVars()) {
                b.put(v, new BackendBindings.IdValueBackend<ID, VALUE>()
                        .setBackend(context.backend)
                        .setString(NodeFmtLib.strNT(extend.getVarExprList().getExpr(v)
                                .eval(i, context).asNode())));
            }
            return b;
        });
    }

    @Override
    public Op pause() {
        Op subop = wrapped.pause();
        if (notExecuted(extend.getSubOp(), subop)) return extend;
        if (isDone(subop)) return DONE;
        return OpCloningUtil.clone(extend, subop);
    }
}
