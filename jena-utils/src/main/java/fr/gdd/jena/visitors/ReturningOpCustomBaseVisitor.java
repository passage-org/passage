package fr.gdd.jena.visitors;

import fr.gdd.jena.utils.OpLeftJoinFail;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.List;
import java.util.function.Function;

/**
 * A visitor dedicated to returning `Op`. Useful for building back plans.
 */
public class ReturningOpCustomBaseVisitor extends ReturningOpVisitor<Op> {

    Function<Op, Op> function;

    public ReturningOpCustomBaseVisitor(Function<Op, Op> f) {
        this.function = f;
    }

    public ReturningOpCustomBaseVisitor() {
        this.function = op -> op;
    }

    public void setFunction(Function<Op, Op> f) {
        this.function = f;
    }

    public Op visit(Op op) {
        return function.apply(op);
    }

    @Override
    public Op visit(OpService req) {
        return function.apply(req);
    }

    @Override
    public Op visit(OpTriple triple) {
        return function.apply(triple);
    }

    @Override
    public Op visit(OpQuad quad) {
        return function.apply(quad);
    }

    @Override
    public Op visit(OpQuadBlock block) {
        return function.apply(block);
    }

    @Override
    public Op visit(OpQuadPattern quads) { return function.apply(quads); }

    @Override
    public Op visit(OpGraph graph) {
        return function.apply(graph);
    }

    @Override
    public Op visit(OpBGP bgp) {
        return function.apply(bgp);
    }

    @Override
    public Op visit(OpSequence sequence) {
        return function.apply(sequence);
    }

    @Override
    public Op visit(OpTable table) {
        return function.apply(table);
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        if(lj instanceof OpLeftJoinFail)
            return function.apply(lj);
        return function.apply(lj);
    }

    @Override
    public Op visit(OpConditional cond) {
        return function.apply(cond);
    }

    @Override
    public Op visit(OpFilter filter) {
        return function.apply(filter);
    }

    @Override
    public Op visit(OpUnion union) {
       return function.apply(union);
    }

    @Override
    public Op visit(OpJoin join) {
        return function.apply(join);
    }

    @Override
    public Op visit(OpDistinct distinct) {
        return function.apply(distinct);
    }

    @Override
    public Op visit(OpSlice slice) {
        return function.apply(slice);
    }

    @Override
    public Op visit(OpOrder orderBy) {
        return function.apply(orderBy);
    }

    @Override
    public Op visit(OpProject project) {
        return function.apply(project);
    }

    @Override
    public Op visit(OpGroup groupBy) {
        return function.apply(groupBy);
    }

    @Override
    public Op visit(OpExtend extend) {
        return function.apply(extend);
    }

    /**
     * Visit all children and apply the visitor.
     * @param children The children to visit.
     * @return List of `Op` resulting from the visit.
     */
    public List<Op> visit(List<Op> children) {
        return children.stream().map(c -> ReturningOpVisitorRouter.visit(this, c)).toList();
    }
}
