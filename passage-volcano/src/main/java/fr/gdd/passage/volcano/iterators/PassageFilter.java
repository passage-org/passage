package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class PassageFilter<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final OpFilter filter;
    final Iterator<BackendBindings<ID,VALUE>> wrapped;

    final BackendBindings<ID,VALUE> differentOf = new BackendBindings<>();
    BackendBindings<ID,VALUE> lastProduced;

    public PassageFilter(PassageOpExecutor<ID,VALUE> executor, OpFilter filter, Iterator<BackendBindings<ID,VALUE>> input) {
        this.filter = filter;
        this.wrapped = input;
        for (E_NotEquals neq : unfoldOrExpr(filter.getExprs().get(0))) {
            differentOf.put(neq.getArg1().asVar(), new BackendBindings.IdValueBackend<ID,VALUE>()
                    .setBackend(executor.getBackend())
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
