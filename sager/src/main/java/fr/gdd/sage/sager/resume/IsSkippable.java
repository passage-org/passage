package fr.gdd.sage.sager.resume;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Visits a query or a subquery and states if the OFFSET operation
 * can use a fast skip algorithm or not.
 */
public class IsSkippable extends ReturningOpVisitor<Boolean> {

    Integer nbTriplePatterns = 0;
    OpTriple opTriple = null;

    public OpTriple getOpTriple() {
        return opTriple;
    }

    public static Boolean visit(Op op) {
        if (op instanceof OpSlice slice) { // The root slice
            return ReturningOpVisitorRouter.visit(new IsSkippable(), slice.getSubOp());
        }
        return false;
    }

    @Override
    public Boolean visit(OpTriple triple) {
        ++nbTriplePatterns;
        opTriple = triple;
        return nbTriplePatterns <= 1;
    }

    @Override
    public Boolean visit(OpBGP bgp) {
        return false;
    }

    @Override
    public Boolean visit(OpExtend extend) {
        return true;
    }

    @Override
    public Boolean visit(OpJoin join) {
        return ReturningOpVisitorRouter.visit(this, join.getLeft()) &&
                ReturningOpVisitorRouter.visit(this, join.getRight());
    }

    @Override
    public Boolean visit(OpFilter filter) {
        return false;
    }

    @Override
    public Boolean visit(OpUnion union) {
        return ReturningOpVisitorRouter.visit(this, union.getLeft()) &&
                ReturningOpVisitorRouter.visit(this, union.getRight());
    }

    @Override
    public Boolean visit(OpSlice slice) {
        return ReturningOpVisitorRouter.visit(this, slice.getSubOp());
    }
}
