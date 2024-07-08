package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpSlice;

/**
 * Need to get the top most OpGroup to get the Aggregator associated.
 */
public class GetRootAggregator extends ReturningOpVisitor<OpGroup> {

    public OpGroup visit(String opAsString) { return ReturningOpVisitorRouter.visit(this, Algebra.compile(QueryFactory.create(opAsString))); }
    public OpGroup visit(Op op) { return ReturningOpVisitorRouter.visit(this, op); }

    @Override
    public OpGroup visit(OpProject project) {
        return ReturningOpVisitorRouter.visit(this, project.getSubOp());
    }

    @Override
    public OpGroup visit(OpExtend extend) {
        return ReturningOpVisitorRouter.visit(this, extend.getSubOp());
    }

    @Override
    public OpGroup visit(OpSlice slice) {
        return ReturningOpVisitorRouter.visit(this, slice.getSubOp());
    }

    @Override
    public OpGroup visit(OpGroup groupBy) {
        return groupBy;
    }
}
