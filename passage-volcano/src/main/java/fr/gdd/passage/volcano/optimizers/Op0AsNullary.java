package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.Op0;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.NullaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;
import se.liu.ida.hefquin.engine.queryplan.physical.NullaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalOperatorForLogicalOperator;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;

public class Op0AsNullary extends LogicalOperatorBase implements NullaryLogicalOp, NullaryPhysicalOp, PhysicalOperatorForLogicalOperator {

    final Op0 op;

    public Op0AsNullary(Op0 op) {
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
    public void visit(PhysicalPlanVisitor visitor) {

    }

    @Override
    public void visit(LogicalPlanVisitor visitor) {
        // Do nothing
        // throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NullaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        return null;
    }

    @Override
    public LogicalOperator getLogicalOperator() {
        return this;
    }

}
