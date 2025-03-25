package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.Op1;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.UnaryLogicalOp;

public class Op1AsUnaryLogicalOp implements UnaryLogicalOp {

    final Op1 op;

    public Op1AsUnaryLogicalOp(Op1 op) {
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
