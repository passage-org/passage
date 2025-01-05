package fr.gdd.passage.volcano.querypatterns;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

/**
 * A DISTINCT of a subquery may or may not be rewritable as a series of
 * DISTINCT of a series of triple/quad pattern.
 * The test consists in checking in
 * TODO for tp
 * TODO for bgp
 */
public class isDistinctableQuery extends ReturningOpVisitor<Boolean> {

    OpProject project;

    @Override
    public Boolean visit(Op op) {
        try {
            return super.visit(op);
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }

    @Override
    public Boolean visit(OpDistinct distinct) {return super.visit(distinct.getSubOp());}

    @Override
    public Boolean visit(OpProject project) {
        this.project = project;
        return super.visit(project.getSubOp());
    }

    @Override
    public Boolean visit(OpTriple triple) {return true;} // ofc

    @Override
    public Boolean visit(OpQuad quad) {return true;} // ofc

    @Override
    public Boolean visit(OpBGP bgp) {
        // TODO check if all triple/quad patterns are linked together by variables
        //      that happen to be in the DISTINCT clause.
        throw new UnsupportedOperationException("DISTINCT BGP Not supported yet.");
    }
}
