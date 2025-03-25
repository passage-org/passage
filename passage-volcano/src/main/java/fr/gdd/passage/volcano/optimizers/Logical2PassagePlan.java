package fr.gdd.passage.volcano.optimizers;

import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalToPhysicalPlanConverter;
import se.liu.ida.hefquin.engine.queryplan.utils.PhysicalPlanFactory;

public class Logical2PassagePlan implements LogicalToPhysicalPlanConverter {

    @Override
    public PhysicalPlan convert(LogicalPlan lp, boolean keepMultiwayJoins) {
        return switch (lp.getRootOperator()) {
            case LogicalOpBGP lBGP -> PhysicalPlanFactory.createPlan(new PhysicalOpBGP(lBGP));
            default -> throw new UnsupportedOperationException("Not supported yet.");
        };
    }
}
