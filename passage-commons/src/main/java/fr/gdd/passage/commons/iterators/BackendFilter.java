package fr.gdd.passage.commons.iterators;

import fr.gdd.passage.commons.engines.BackendPullExecutor;
import fr.gdd.passage.commons.factories.IBackendFiltersFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Filter iterator, often requires to evaluate the result, therefore needs to materialize
 * it instead of using the identifier, which might prove less efficient.
 */
public class BackendFilter<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendFiltersFactory<ID,VALUE> factory() {
        return (context, input, filter) -> {
            BackendPullExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            return new BackendFilter<>(executor, input, filter);
        };
    }

    /* ******************************* ACTUAL FILTER ******************************** */

    final BackendPullExecutor<ID,VALUE> executor;
    final OpFilter filter;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;

    BackendBindings<ID,VALUE> lastProduced;

    public BackendFilter(BackendPullExecutor<ID,VALUE> executor, Iterator<BackendBindings<ID,VALUE>> input, OpFilter filter) {
        this.executor = executor;
        this.filter = filter;
        this.wrapped = executor.visit(filter.getSubOp(), input); // The input is bypassed, then filter applies
    }

    @Override
    public boolean hasNext() {
        if (Objects.nonNull(lastProduced)) return true;

        while (wrapped.hasNext()) {
            BackendBindings<ID,VALUE> produced = wrapped.next();

            if (filter.getExprs().isSatisfied(produced, this.executor.context)) {
                lastProduced = produced;
                return true;
            }
        }

        return false;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        BackendBindings<ID,VALUE> consumed = lastProduced;
        lastProduced = null;
        return consumed;
    }

    /* ***************************************************************** */

    private List<E_NotEquals> unfoldOrExpr (Expr expr) {
        return switch (expr) {
            case E_NotEquals neq -> new ArrayList<>(List.of(neq));
            case E_LogicalOr or -> {
                List<E_NotEquals> left = unfoldOrExpr(or.getArg1());
                left.addAll(unfoldOrExpr(or.getArg2()));
                yield left;
            }
            default -> throw new UnsupportedOperationException("Unsupported expression in filter: " + expr);
        };
    }
}
