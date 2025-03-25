package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNullaryRootImpl;

/**
 * From one kind of Op to the other.
 */
public class Jena2HeFQUIN {

    public static LogicalPlan convert(Op jena) {
        return switch (jena) {
            case OpBGP bgp -> new LogicalPlanWithNullaryRootImpl(new LogicalOpBGP(bgp));
            default -> throw new UnsupportedOperationException("Not supported yet.");
        };
    }

}
