package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.Op0;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.NullaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;

public class Op0AsNullaryLogicalOp extends LogicalOperatorBase implements NullaryLogicalOp {

    final Op0 op;

    public Op0AsNullaryLogicalOp(Op0 op) {
        this.op = op;
    }

    public Op0 getOp() {
        return op;
    }

    @Override
    public ExpectedVariables getExpectedVariables(ExpectedVariables... inputVars) {
        return null;
    }

    @Override
    public int getID() {
        return 0;
    }

    @Override
    public void visit(LogicalPlanVisitor visitor) {
        // Do nothing
        // throw new UnsupportedOperationException("Not supported yet.");
    }
}
