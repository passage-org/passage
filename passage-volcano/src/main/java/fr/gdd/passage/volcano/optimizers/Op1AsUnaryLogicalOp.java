package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.Op1;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.UnaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;

public class Op1AsUnaryLogicalOp extends LogicalOperatorBase implements UnaryLogicalOp {

    final Op1 op;

    public Op1AsUnaryLogicalOp(Op1 op) {
        this.op = op;
    }

    public Op1 getOp() {
        return op;
    }

    @Override
    public ExpectedVariables getExpectedVariables(ExpectedVariables... inputVars) {
        throw new UnsupportedOperationException("Not supported yet.");
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
