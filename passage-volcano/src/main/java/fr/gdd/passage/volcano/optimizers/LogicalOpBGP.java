package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.op.OpBGP;
import se.liu.ida.hefquin.base.query.impl.BGPImpl;
import se.liu.ida.hefquin.base.queryplan.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanUtils;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanVisitor;
import se.liu.ida.hefquin.engine.queryplan.logical.NullaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpBGPAdd;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOperatorBase;

public class LogicalOpBGP extends LogicalOperatorBase implements NullaryLogicalOp {

    final OpBGP opBGP;

    public LogicalOpBGP(OpBGP opBGP) {
        this.opBGP = opBGP;
    }

    @Override
    public ExpectedVariables getExpectedVariables(ExpectedVariables... inputVars) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void visit(LogicalPlanVisitor visitor) {
        // if (visitor instanceof LogicalPlanUtils.LogicalPlanCounter counter) {
        //    counter.visit(new LogicalOpBGPAdd(,));
        // }
        // throw new UnsupportedOperationException("Not supported yet.");
    }
}
