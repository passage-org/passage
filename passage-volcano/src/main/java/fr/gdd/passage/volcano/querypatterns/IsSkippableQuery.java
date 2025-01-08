package fr.gdd.passage.volcano.querypatterns;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Visits a query or a subquery and states if the OFFSET operation
 * can use a fast skip algorithm or not. Not every operation are allowed skipping.
 * Mostly triple patterns and quad patterns, along with direct modifiers on them, e.g.
 * bounded variables.
 */
public class IsSkippableQuery extends ReturningOpVisitor<Boolean> {

    static private final Boolean NOT_ALLOWED = false;
    Integer nbTPQP = 0;
    Op0 tripleOrQuad = null;
    Boolean canBeSkipped = false;

    public Op0 getTripleOrQuad() {
        return tripleOrQuad;
    }

    @Override
    public Boolean visit(Op op) {
        nbTPQP = 0; // reset
        tripleOrQuad = null; // reset

        if (op instanceof OpSlice slice) { // The root slice
            try {
                return super.visit(slice.getSubOp()); // call to super to reroute the operation
            } catch (UnsupportedOperationException e) {
                return NOT_ALLOWED; // avoid implementing all since it throws by default
            }
        }
        return NOT_ALLOWED; // only OFFSET can skip bindings.
    }

    public Boolean cantIt() {
        return canBeSkipped;
    }

    @Override
    public Boolean visit(OpTriple triple) {
        ++nbTPQP;
        tripleOrQuad = triple;
        return nbTPQP <= 1;
    }

    @Override
    public Boolean visit(OpQuad quad) {
        ++nbTPQP;
        tripleOrQuad = quad;
        return nbTPQP <= 1;
    }

    @Override
    public Boolean visit(OpBGP bgp) {
        nbTPQP += bgp.getPattern().getList().size();
        return nbTPQP <= 1;
    }

    @Override
    public Boolean visit(OpExtend extend) { return super.visit(extend.getSubOp()); }

    @Override
    public Boolean visit(OpProject project) {
        return super.visit(project.getSubOp());
    }

    @Override
    public Boolean visit(OpJoin join) { // both side must allow skipping
        return super.visit(join.getLeft()) && super.visit(join.getRight());
    }

    @Override
    public Boolean visit(OpTable table) {
        if (table.isJoinIdentity() || table.getTable().size() <= 1) { return true; }
        return NOT_ALLOWED; // otherwise, it means multiple values to explore, so false
    }

    @Override
    public Boolean visit(OpSlice slice) { return NOT_ALLOWED; } // root slice already visited at this stage

    @Override
    public Boolean visit(OpDistinct distinct) {
        return super.visit(distinct.getSubOp());
    }
}
