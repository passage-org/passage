package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.OpN;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.NaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;

public class OpNAsNAryLogicalOp extends LogicalOperatorBase implements NaryLogicalOp {

    final OpN op;

    public OpNAsNAryLogicalOp(OpN op) {
        this.op = op;
    }

    public OpN getOp() {
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
