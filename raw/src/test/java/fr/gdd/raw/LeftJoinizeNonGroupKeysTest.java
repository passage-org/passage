package fr.gdd.raw;

import fr.gdd.jena.utils.OpLeftJoinFail;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class LeftJoinizeNonGroupKeysTest {

    String queryNominal = "SELECT * WHERE {?s ?p ?o.}";

    @Test
    public void testLeftJoinizeNonGroupKeys() {
        Op op = Algebra.compile(QueryFactory.create(queryNominal));

        ReturningOpVisitor<Op> visitor = new LeftJoinizeNonGroupKeys();

        Op expectedOp = ReturningOpVisitorRouter.visit(visitor, op);

        Assert.assertTrue(expectedOp instanceof OpLeftJoinFail);
        Assert.assertTrue(((OpLeftJoinFail) expectedOp).getLeft() instanceof OpTable);
        Assert.assertTrue(((OpTable) ((OpLeftJoinFail) expectedOp).getLeft()).isJoinIdentity());

    }
}
