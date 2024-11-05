package fr.gdd.passage.commons.iterators;

import fr.gdd.passage.commons.factories.IBackendBindsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.atlas.iterator.Iter;
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
import java.util.Objects;

/**
 * We focus on BIND(<https://that_exists_in_db> AS ?variable). Expressions are as simple as that.
 */
public class BackendBind<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendBindsFactory<ID,VALUE> factory() {
        return (context, input, extend) -> {
            Backend<ID,VALUE,?> backend = context.getContext().get(BackendConstants.BACKEND);
            BackendCache<ID,VALUE> cache = context.getContext().get(BackendConstants.CACHE);
            BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            return new BackendBindsFactory<>(executor, input, extend, backend, cache, context);
        };
    }

    /* ************************** ITERATOR FOR EACH INPUT ************************** */

    public static class BackendBindsFactory<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

        final Iterator<BackendBindings<ID,VALUE>> input;
        final ExecutionContext context;
        final Backend<ID,VALUE,?> backend;
        final BackendCache<ID,VALUE> cache;
        final BackendOpExecutor<ID,VALUE> executor;
        final OpExtend op;

        Iterator<BackendBindings<ID,VALUE>> current;

        public BackendBindsFactory(BackendOpExecutor<ID,VALUE> executor,
                                   Iterator<BackendBindings<ID,VALUE>> input, OpExtend op,
                                   Backend<ID,VALUE,?> backend, BackendCache<ID, VALUE> cache, ExecutionContext context) {
            this.executor = executor;
            this.backend = backend;
            this.input = input;
            this.cache = cache;
            this.context = context;
            this.op = op;
        }

        @Override
        public boolean hasNext() {
            if (Objects.isNull(current) && !input.hasNext()) return false;

            if (Objects.nonNull(current) && current.hasNext()) return true;

            while (Objects.isNull(current) && input.hasNext()) {
                BackendBindings<ID, VALUE> inputBinding = input.next();
                current = new BackendBind<>(executor, inputBinding, op, backend, cache, context);
                if (!current.hasNext()) {
                    current = null;
                }
            }

            if (Objects.isNull(current)) return false;

            return current.hasNext();
        }

        @Override
        public BackendBindings<ID, VALUE> next() {
            return current.next();
        }
    }

    /* ******************************** ACTUAL ITERATOR ********************************* */

    final BackendBindings<ID,VALUE> input;
    final ExecutionContext context;
    final VarExprList exprs;
    final Backend<ID,VALUE,?> backend;
    final BackendCache<ID,VALUE> cache;

    final Iterator<BackendBindings<ID,VALUE>> wrapped;

    public BackendBind(BackendOpExecutor<ID,VALUE> executor, BackendBindings<ID,VALUE> input, OpExtend op,
                       Backend<ID,VALUE,?> backend, BackendCache<ID, VALUE> cache, ExecutionContext context) {
        this.exprs =  op.getVarExprList();
        this.context = context;
        this.backend = backend;
        this.cache = cache;
        this.input = eval(input);
        this.wrapped = executor.visit(op.getSubOp(), Iter.of(this.input));
    }

    @Override
    public boolean hasNext() {
        return wrapped.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return wrapped.next();
    }

    public BackendBindings<ID,VALUE> eval(BackendBindings<ID,VALUE> current) {
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
