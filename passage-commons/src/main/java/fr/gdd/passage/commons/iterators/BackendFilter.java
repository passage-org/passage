package fr.gdd.passage.commons.iterators;

import fr.gdd.passage.commons.factories.IBackendFiltersFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class BackendFilter<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    public static <ID,VALUE> IBackendFiltersFactory<ID,VALUE> factory() {
        return (context, input, filter) -> {
            Backend<ID,VALUE,?> backend = context.getContext().get(BackendConstants.BACKEND);
            return new BackendFilter<>(backend, input, filter);
        };
    }

    final OpFilter filter;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;

    final BackendBindings<ID,VALUE> differentOf = new BackendBindings<>();
    BackendBindings<ID,VALUE> lastProduced;

    public BackendFilter(Backend<ID,VALUE,?> backend, Iterator<BackendBindings<ID,VALUE>> input, OpFilter filter) {
        this.filter = filter;
        this.wrapped = input;
        for (E_NotEquals neq : unfoldOrExpr(filter.getExprs().get(0))) {
            differentOf.put(neq.getArg1().asVar(), new BackendBindings.IdValueBackend<ID,VALUE>()
                    .setBackend(backend)
                    .setString(neq.getArg2().toString()));
        }
    }

    @Override
    public boolean hasNext() {
        if (Objects.nonNull(lastProduced)) return true;

        while (wrapped.hasNext()) {
            BackendBindings<ID,VALUE> produced = wrapped.next();
            if (!produced.equals(differentOf)) {
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
