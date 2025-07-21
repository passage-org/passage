package fr.gdd.passage.volcano.transforms;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * When a filter as a list of expressions that are duplicated, we
 * only keep one duplicate.
 */
public class DeduplicateFilters extends TransformCopy {

    public DeduplicateFilters() {}

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        ExprList exprList = opFilter.getExprs();
        HashSet<Expr> exprs = new HashSet<>(exprList.getList());
        ExprList newExprList = new ExprList();
        exprs.forEach(newExprList::add);
        return OpFilter.filterDirect(newExprList, subOp) ;
    }
}
