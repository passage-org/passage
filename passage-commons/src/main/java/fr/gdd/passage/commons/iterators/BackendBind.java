package fr.gdd.passage.commons.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.factories.IBackendBindsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.E_Add;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;

/**
 * We focus on BIND(<https://that_exists_in_db> AS ?variable). Expressions are as simple as that.
 */
public class BackendBind<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendBindsFactory<ID,VALUE> factory() {
        return (context, input, extend) -> {
            Backend<ID,VALUE,?> backend = context.getContext().get(BackendConstants.BACKEND);
            BackendCache<ID,VALUE> cache = context.getContext().get(BackendConstants.CACHE);
            BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            Iterator<BackendBindings<ID,VALUE>> newInput = ReturningArgsOpVisitorRouter.visit(executor, extend.getSubOp(), input);
            return new BackendBind<>(newInput, extend, backend, cache, context);
        };
    }

    final Iterator<BackendBindings<ID,VALUE>> input;
    final ExecutionContext context;
    final VarExprList exprs;
    final Backend<ID,VALUE,?> backend;
    final BackendCache<ID,VALUE> cache;

    public BackendBind(Iterator<BackendBindings<ID,VALUE>> input, OpExtend op,
                       Backend<ID,VALUE,?> backend, BackendCache<ID, VALUE> cache, ExecutionContext context) {
        this.exprs =  op.getVarExprList();
        this.context = context;
        this.input = input;
        this.backend = backend;
        this.cache = cache;
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public BackendBindings<ID,VALUE> next() {
        BackendBindings<ID,VALUE> current = input.next();
        BackendBindings<ID,VALUE> b = new BackendBindings<ID,VALUE>().setParent(current);

        for (Var v : exprs.getVars()) {
            Expr expr = exprs.getExpr(v);
            // NodeValue nv = expr.eval(Binding.noParent, context); // basic expressions only
            BackendBindings.IdValueBackend<ID,VALUE> newBinding = new BackendBindings.IdValueBackend<ID,VALUE>()
                    .setBackend(backend);
            if (expr.isVariable()) {
                newBinding.setValue(b.getBinding(expr.asVar()).getValue());
            } else if (expr.isConstant()) {
                if (expr instanceof NodeValue nv) {
                    newBinding.setId(cache.getId(nv.getNode())); // The id might already exist in cache
                }
                newBinding.setString(expr.toString()); // try subject
            } else if (expr instanceof E_Add add) {
                String binding = current.getBinding(add.getArg1().asVar()).getString(); // substr because it has ""
                binding = binding.substring(1, binding.length()-1); // ugly af

                NodeValueInteger right = (NodeValueInteger) add.getArg2();

                NodeValue newValue = ExprUtils.eval(new E_Add(right, (NodeValueInteger) NodeValue.parse(binding)));
                newBinding.setString(newValue.asString());
            }

            b.put(v, newBinding);
        }

        return b;
    }
}
