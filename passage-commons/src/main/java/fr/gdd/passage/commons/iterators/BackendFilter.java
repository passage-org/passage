package fr.gdd.passage.commons.iterators;

import fr.gdd.passage.commons.factories.IBackendFiltersFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendOpExecutor;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.commons.collections4.iterators.EmptyIterator;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A very specific filter that only works for expressions of NOT_EQUAL, and OR, such as:
 * `FILTER (?x != "some_value" || ?x != <other_value>)`
 */
public class BackendFilter<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendFiltersFactory<ID,VALUE> factory() {
        return (context, input, filter) -> {
            Backend<ID,VALUE,?> backend = context.getContext().get(BackendConstants.BACKEND);
            BackendOpExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            return new BackendFilter<>(executor, backend, input, filter);
        };
    }

    /* ******************************* ACTUAL FILTER ******************************** */

    final BackendOpExecutor<ID,VALUE> executor;
    final OpFilter filter;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;

    final BackendBindings<ID,VALUE> differentOf = new BackendBindings<>();
    BackendBindings<ID,VALUE> lastProduced;

    public BackendFilter(BackendOpExecutor<ID,VALUE> executor, Backend<ID,VALUE,?> backend, Iterator<BackendBindings<ID,VALUE>> input, OpFilter filter) {
        this.executor = executor;
        this.filter = filter;
        this.wrapped = executor.visit(filter.getSubOp(), input); // The input is bypassed, then filter applies
//        for (E_NotEquals neq : unfoldOrExpr(filter.getExprs().get(0))) {
//            differentOf.put(neq.getArg1().asVar(), new BackendBindings.IdValueBackend<ID,VALUE>()
//                    .setBackend(backend)
//                    .setString(neq.getArg2().toString()));
//        }
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
