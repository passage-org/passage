package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.OpN;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.NaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.NaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;
import se.liu.ida.hefquin.engine.queryplan.physical.NaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalOperatorForLogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class OpNAsNAry extends LogicalOperatorBase implements NaryLogicalOp, NaryPhysicalOp, PhysicalOperatorForLogicalOperator {

    final OpN op;

    public OpNAsNAry(OpN op) {
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
    public void visit(PhysicalPlanVisitor visitor) {

    }

    @Override
    public void visit(LogicalPlanVisitor visitor) {
        // Do nothing
        // throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        return null;
    }

    @Override
    public LogicalOperator getLogicalOperator() {
        return this;
    }
}
