package fr.gdd.passage.volcano.optimizers;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import se.liu.ida.hefquin.base.query.impl.GenericSPARQLGraphPatternImpl2;
import se.liu.ida.hefquin.engine.queryplan.logical.NullaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNullaryRootImpl;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalToPhysicalPlanConverter;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalToPhysicalPlanConverterImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.EquilibriumConditionByRelativeSubplanCount;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.SimulatedAnnealing;

public class OpOrderingUsingHeFQUIN {

    @Disabled
    @Test
    public void start_testing_an_approach_to_support_hefquins_optimizers () {
        // TODO have a logical to physical converted.
        Op query = Algebra.compile(QueryFactory.create("SELECT * WHERE { ?s ?p ?o }"));
        var meow = new GenericSPARQLGraphPatternImpl2(query);

        LogicalPlanWithNullaryRootImpl lp = new LogicalPlanWithNullaryRootImpl((NullaryLogicalOp) query);
        // LogicalToPhysicalPlanConverter l2pConverter = new LogicalToPhysicalPlanConverterImpl();
        // new SimulatedAnnealing(new EquilibriumConditionByRelativeSubplanCount(), );
    }

}
