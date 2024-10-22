package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.factories.IBackendDistinctsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.volcano.PassageConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A distinct iterator that only check if the last one is the same, because it assumes
 * an order by where bindings that are identical are produced are contiguous.
 * Only works in this case, which is enough for us for now.
 */
public class PassageDistinct<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendDistinctsFactory<ID,VALUE> factory() {
        return (context, input, distinct) -> {
            BackendSaver<ID,VALUE,?> saver = context.getContext().get(PassageConstants.SAVER);
            BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            Iterator<BackendBindings<ID,VALUE>> wrapped = executor.visit(distinct.getSubOp(), input);
            Iterator<BackendBindings<ID,VALUE>> distincts = new PassageDistinct<>(distinct, context, wrapped);
            saver.register(distinct, distincts);
            return distincts;
        };
    }

    final ExecutionContext context;
    final OpDistinct op;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;

    BackendBindings<ID,VALUE> lastBinding = new BackendBindings<>();
    BackendBindings<ID,VALUE> newBinding;

    public PassageDistinct(OpDistinct op, ExecutionContext context, Iterator<BackendBindings<ID,VALUE>> wrapped) {
        this.context = context;
        this.op = op;
        this.wrapped = wrapped;
    }

    @Override
    public boolean hasNext() {
        if (Objects.nonNull(newBinding)) return true;

        while (wrapped.hasNext()) {
            BackendBindings<ID,VALUE> produced = wrapped.next();
            if (!produced.equals(lastBinding)) {
                newBinding = produced;
                return true;
            }
        }

        return false;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        lastBinding = newBinding;
        newBinding = null; // consumed
        return lastBinding;
    }

    public Op getFilter(Op subop) {
        ExprList exprs = new ExprList();

        // TODO use E_NotEquals and E_LogicalOr
        String expr = lastBinding.vars().stream().map(
                v-> String.format("%s != %s", v, lastBinding.get(v).getString())
        ).collect(Collectors.joining( " || "));

        for (Var v : lastBinding.vars()) {
            String exprAsString = String.format("%s != %s", v, lastBinding.get(v).getString());
            exprs.add(ExprUtils.parse(exprAsString));// ugly but whatever
        }

        return OpFilter.filter(ExprUtils.parse(expr), subop); // if empty, it returns subop
    }
}
