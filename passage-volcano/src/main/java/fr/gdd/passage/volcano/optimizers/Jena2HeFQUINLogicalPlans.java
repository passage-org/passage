package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithBinaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNullaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithUnaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalToPhysicalPlanConverter;
import se.liu.ida.hefquin.engine.queryplan.utils.PhysicalPlanFactory;

/**
 * Jena has its set of operators; HeFQUIN has it set of operators. This utility class aims to
 * ease the convertion from one to the other.
 */
public class Jena2HeFQUINLogicalPlans implements LogicalToPhysicalPlanConverter {

    /**
     * @param jena The operator to convert from.
     * @return An HeFQUIN logical plan with the appropriate arity.
     */
    public static LogicalPlan convert(Op jena) {
        return switch (jena) {
            case Op0 op0 -> new LogicalPlanWithNullaryRootImpl(new Op0AsNullaryLogicalOp(op0));
            case Op1 op1 -> new LogicalPlanWithUnaryRootImpl(new Op1AsUnaryLogicalOp(op1), convert(op1.getSubOp()));
            case Op2 op2 -> new LogicalPlanWithBinaryRootImpl(new Op2AsBinaryLogicalOp(op2),
                    convert(op2.getLeft()), convert(op2.getRight()));
            case OpN opn -> new LogicalPlanWithNaryRootImpl(new OpNAsNAryLogicalOp(opn),
                    opn.getElements().stream().map(Jena2HeFQUINLogicalPlans::convert).toList());
            default -> throw new UnsupportedOperationException("There should not exist other type of operators.");
        };
    }

    /**
     * @param logical The logical HeFQUIN plan.
     * @return A physical plan wrapping up a physical operator built from logical plan wrapping Jena's operators.
     */
    public static PhysicalPlan convert(LogicalPlan logical) {
        return switch (logical.getRootOperator()) {
            case Op0AsNullaryLogicalOp op0 -> switch (op0.getOp()) {
                case OpBGP ignored -> PhysicalPlanFactory.createPlan(new PhysicalOpBGP(op0));
                default -> throw new UnsupportedOperationException("This Op0 is not supported yet.");
            };
            default -> throw new UnsupportedOperationException("This Op is not supported yet.");
        };
    }

    @Override
    public PhysicalPlan convert(LogicalPlan lp, boolean keepMultiwayJoins) {
        return convert(lp);
    }
}
