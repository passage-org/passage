package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.Op1;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.UnaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.UnaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalOperatorForLogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.physical.UnaryPhysicalOp;

public class Op1AsUnary extends LogicalOperatorBase implements UnaryLogicalOp, UnaryPhysicalOp, PhysicalOperatorForLogicalOperator {

    final Op1 op;

    public Op1AsUnary(Op1 op) {
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
    public void visit(PhysicalPlanVisitor visitor) {

    }

    @Override
    public void visit(LogicalPlanVisitor visitor) {
        // Do nothing
        // throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public UnaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        return null;
    }

    @Override
    public LogicalOperator getLogicalOperator() {
        return this;
    }
}
