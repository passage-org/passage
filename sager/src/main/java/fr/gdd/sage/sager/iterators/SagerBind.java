package fr.gdd.sage.sager.iterators;

import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.sager.SagerConstants;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;

import java.util.Iterator;

/**
 * We focus on BIND(<http://that_exists_in_db> AS ?variable). Expressions are as simple as that.
 */
public class SagerBind<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final Iterator<BackendBindings<ID,VALUE>> input;
    final ExecutionContext context;
    final VarExprList exprs;
    final Backend<ID,VALUE,?> backend;

    public SagerBind(Iterator<BackendBindings<ID,VALUE>> input, OpExtend op, ExecutionContext context) {
        this.exprs =  op.getVarExprList();
        this.context = context;
        this.input = input;
        this.backend = context.getContext().get(SagerConstants.BACKEND);
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public BackendBindings<ID,VALUE> next() {
        BackendBindings<ID,VALUE> current = input.next();
        BackendBindings<ID,VALUE> b = new BackendBindings<ID,VALUE>().setParent(current);
        // BackendBindings<ID,VALUE> b = new BindingId2Value().setParent(current).setDefaultTable(current.getDefaultTable());

        for (Var v : exprs.getVars()) {
            Expr expr = exprs.getExpr(v);
            // NodeValue nv = expr.eval(Binding.noParent, context); // basic expressions only
            BackendBindings.IdValueBackend<ID,VALUE> newBinding = new BackendBindings.IdValueBackend<ID,VALUE>()
                    .setBackend(backend);
            if (expr.isVariable()) {
                newBinding.setValue(b.get(expr.asVar()).getValue());
            } else {
                newBinding.setString(expr.toString()); // try subject
            }

            b.put(v, newBinding);
//            if (Objects.isNull(expr)) {
//                b.put(v, b.getId(v));
//            } else {
//            try {
//                NodeValue nv = expr.eval(b, context);
//                if (Objects.nonNull(nv))
//                    b.put(v, nv.asNode());
//            } catch (ExprEvalException ex) {}
            //}
        }

        return b;
    }
}
