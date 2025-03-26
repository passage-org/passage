package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.Op2;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.logical.BinaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;

public class Op2AsBinaryLogicalOp extends LogicalOperatorBase implements BinaryLogicalOp {

    final Op2 op;

    public Op2AsBinaryLogicalOp(Op2 op) {
        this.op = op;
    }

    public Op2 getOp() {
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
