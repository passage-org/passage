package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpN;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithBinaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNullaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithUnaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.physical.*;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalToPhysicalPlanConverter;
import se.liu.ida.hefquin.engine.queryplan.utils.PhysicalPlanFactory;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

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
            case Op0 op0 -> new LogicalPlanWithNullaryRootImpl(new Op0AsNullary(op0));
            case Op1 op1 -> new LogicalPlanWithUnaryRootImpl(new Op1AsUnary(op1), convert(op1.getSubOp()));
            case Op2 op2 -> new LogicalPlanWithBinaryRootImpl(new Op2AsBinary(op2),
                    convert(op2.getLeft()), convert(op2.getRight()));
            case OpN opn -> new LogicalPlanWithNaryRootImpl(new OpNAsNAry(opn),
                    opn.getElements().stream().map(Jena2HeFQUINLogicalPlans::convert).toList());
            default -> throw new UnsupportedOperationException("There should not exist other type of operators.");
        };
    }

    /**
     * @param logical The logical HeFQUIN plan.
     * @return A physical plan wrapping up a physical operator built from logical plan wrapping Jena's operators.
     *         which is in fact, itself…
     */
    public static PhysicalPlan convert(LogicalPlan logical) {
        return switch (logical) {
            case LogicalPlanWithNullaryRootImpl p0 -> PhysicalPlanFactory.createPlan((NullaryPhysicalOp) p0.getRootOperator());
            case LogicalPlanWithUnaryRootImpl p1 -> PhysicalPlanFactory.createPlan((UnaryPhysicalOp) p1.getRootOperator(), convert(p1.getSubPlan()));
            case LogicalPlanWithBinaryRootImpl p2 -> PhysicalPlanFactory.createPlan((BinaryPhysicalOp) p2.getRootOperator(), convert(p2.getSubPlan1()), convert(p2.getSubPlan2()));
            case LogicalPlanWithNaryRootImpl pn -> PhysicalPlanFactory.createPlan((NaryPhysicalOp) pn.getRootOperator(),
                            StreamSupport.stream(Spliterators.spliteratorUnknownSize(pn.getSubPlans(), Spliterator.ORDERED), false)
                                    .map(Jena2HeFQUINLogicalPlans::convert).collect(Collectors.toList()));
            default -> throw new UnsupportedOperationException("There should not exist other type of logical plans.");
        };
    }

    /**
     * @param lp The logical plan wrapping Jena's operators.
     * @param keepMultiwayJoins ignored since join are binary by default.
     * @return The physical plan wrapping the logical one.
     */
    @Override
    public PhysicalPlan convert(LogicalPlan lp, boolean keepMultiwayJoins) {
        return convert(lp);
    }


    public static Op convert(PhysicalPlan pp) {
        return switch (pp) {
            case PhysicalPlanWithNullaryRoot p0 -> ((Op0AsNullary) p0.getRootOperator()).getOp();
            case PhysicalPlanWithUnaryRoot p1 -> ((Op1AsUnary) p1.getRootOperator()).getOp().copy(convert(p1.getSubPlan()));
            case PhysicalPlanWithBinaryRoot p2 -> ((Op2AsBinary) p2.getRootOperator()).getOp().copy(convert(p2.getSubPlan1()), convert(p2.getSubPlan2()));
            case PhysicalPlanWithNaryRoot pn -> ((OpNAsNAry) pn.getRootOperator()).getOp().copy(
                    IntStream.rangeClosed(0,pn.numberOfSubPlans()).mapToObj((i)-> convert(pn.getSubPlan(i))).toList());
            default -> throw new UnsupportedOperationException("There should not exist other type of physical plans.");
        };
    }
}
