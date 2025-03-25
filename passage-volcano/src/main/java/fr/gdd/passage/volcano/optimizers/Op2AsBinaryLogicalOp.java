package fr.gdd.passage.volcano.optimizers;

import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.logical.BinaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;

public class Op2AsBinaryLogicalOp implements BinaryLogicalOp {

    final Op2AsBinaryLogicalOp op;

    public Op2AsBinaryLogicalOp(Op2AsBinaryLogicalOp op) {
        this.op = op;
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
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
