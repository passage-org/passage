package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.querypatterns.IsDistinctableQuery;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.expr.E_LogicalOr;
import org.apache.jena.sparql.expr.E_NotEquals;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.Objects;
import java.util.stream.Stream;

@Deprecated // TODO pause
public class PausableStreamDistinct<ID,VALUE> implements PausableStream<ID, VALUE> {

    final OpDistinct distinct;
    final IsDistinctableQuery distinctable;
    final PassageExecutionContext<ID,VALUE> context;
    final BackendBindings<ID,VALUE> input;
    final PausableStream<ID, VALUE> wrapped;
    BackendBindings<ID,VALUE> lastProduced;

    public PausableStreamDistinct(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpDistinct distinct) {
        this.distinct = distinct;
        this.context = context;
        this.input = input;
        this.distinctable = new IsDistinctableQuery();
        if (distinctable.visit((Op) distinct)) {
            if (Objects.isNull(distinctable.project)) {
                // project all so it works by default, no need for specific protocol
                this.wrapped = ((PassagePushExecutor<ID,VALUE>) context.executor).visit(distinct.getSubOp(), input);
            } else {
                // but otherwise, need to be extra careful with the index chosen
                PassageExecutionContext<ID, VALUE> newContext = new PassageExecutionContext<>(((PassageExecutionContext<?, ?>) context).clone());
                newContext.getContext().set(PassageConstants.PROJECT, distinctable.project);
                this.wrapped = new PausableStreamWrapper<>(newContext, input, distinctable.tripleOrQuad, SpliteratorDistinct::new);
            }
        } else {
            throw new UnsupportedOperationException("Distinct for complex queries is not implemented yet.");
        }
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return wrapped.stream().peek(binding -> lastProduced = binding);
    }

    /* ******************************************************************************* */

    @Override
    public Op pause() {
        // {DISTINCT PROJECT {TP OFFSET}} FILTER last value
        Expr expr = getFilterExpr();
        Op paused =  Objects.isNull(expr) ? wrapped.pause() : OpFilter.filterDirect(expr, wrapped.pause());
        return new OpDistinct(OpCloningUtil.clone(distinctable.project, paused));
    }

    public Expr getFilterExpr() {
        if (Objects.isNull(lastProduced)) { return null;}
        return distinctable.project.getVars().stream().map(v ->
                (Expr) new E_NotEquals(ExprUtils.parse(v.toString()), ExprUtils.parse(lastProduced.getBinding(v).getString()))
        ).reduce(null, (left, right) -> Objects.isNull(left) ? right : new E_LogicalOr(left, right));
    }

}
