package fr.gdd.passage.volcano.optimizers;

import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.physical.NullaryPhysicalOp;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.physical.impl.BaseForPhysicalOps;

public class PhysicalOpBGP extends BaseForPhysicalOps implements NullaryPhysicalOp {

    final LogicalOpBGP opBGP;

    PhysicalOpBGP(LogicalOpBGP opBGP) {
        this.opBGP = opBGP;
    }

    @Override
    public NullaryExecutableOp createExecOp(boolean collectExceptions, ExpectedVariables... inputVars) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ExpectedVariables getExpectedVariables(ExpectedVariables... inputVars) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(PhysicalPlanVisitor visitor) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
