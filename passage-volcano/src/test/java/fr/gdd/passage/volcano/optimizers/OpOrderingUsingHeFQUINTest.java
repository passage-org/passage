package fr.gdd.passage.volcano.optimizers;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.LogicalOptimizationException;
import se.liu.ida.hefquin.engine.queryproc.PhysicalOptimizationException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.costmodel.CostDimension;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.costmodel.CostModelImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.EquilibriumConditionByRelativeSubplanCount;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.StoppingConditionByNumberOfGenerations;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.TwoPhaseQueryOptimizer;

import java.util.concurrent.CompletableFuture;

public class OpOrderingUsingHeFQUINTest {

    // TODO create a fake blazegraph graph for wikidata queries.
    // final static String WDBENCH_PATH = "/Users/skoazell/Desktop/Projects/datasets/wdbench-blaze/wdbench-blaze.jnl";
    final static String WDBENCH_PATH = "/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl";

    @Disabled
    @Test
    public void start_testing_an_approach_to_support_hefquins_optimizers () throws SourcePlanningException, PhysicalOptimizationException, LogicalOptimizationException, RepositoryException, SailException {
        // Backend<?,?> backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        Backend<?,?> backend = new BlazegraphBackend(WDBENCH_PATH);
        BackendCardinalityEstimation estimator = new BackendCardinalityEstimation(backend);
        CostDimension[] dimensions = new CostDimension[] {
                new CostDimension(1.0,
                        (visitedPlans, plan) ->  visitedPlans.contains(plan) ?
                                CompletableFuture.completedFuture( 0):
                                estimator.initiateCardinalityEstimation(plan)) // cardinality dimension
        }; // possibly add other dimensions, e.g. using costs

        // Op query = Algebra.compile(QueryFactory.create("SELECT * WHERE  { { ?s ?p ?o } { ?o <http://meow> ?o2 } }"));
        // Op query = Algebra.compile(QueryFactory.create("SELECT * WHERE  { ?s ?p ?o . ?o <http://meow> ?o2 }"));
        Op query = Algebra.compile(QueryFactory.create("""
                SELECT ?item (STR(?label) AS ?itemLabel) WHERE {
                  {
                    ?item <http://www.w3.org/2000/01/rdf-schema#label> ?label.
                    FILTER((((LANG(?label)) = "en") || ((LANG(?label)) = "mul")) || ((LANG(?label)) = "fr"))
                  }
                  ?item <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q146>.
                }"""));
        LogicalPlan lPlan = Jena2HeFQUINLogicalPlans.convert(query);

        // vvvv from ExempleEngineConf.ttl
        StoppingConditionByNumberOfGenerations condition1 = new StoppingConditionByNumberOfGenerations(5);
        // vvvv from ExampleEngineConf.ttl as well
        EquilibriumConditionByRelativeSubplanCount condition2 = new EquilibriumConditionByRelativeSubplanCount(16);

        TwoPhaseQueryOptimizer twoPhaseQueryOpt = new TwoPhaseQueryOptimizer(condition1, condition2,
                /* logical2physicalplanconverter */ new Jena2HeFQUINLogicalPlans(),
                /* costmodel */  new CostModelImpl(dimensions),
                /* rulesinstance */ new SPARQLRuleInstances() // TODO implements the rules
        );

        var pair = twoPhaseQueryOpt.optimize(lPlan);

        /*
        SimulatedAnnealing annealingOpt = new SimulatedAnnealing(condition2,
                new Jena2HeFQUINLogicalPlans(),
                new CostModelImpl(dimensions),
                new SPARQLRuleInstances()
                );

        pair = annealingOpt.optimize(Jena2HeFQUINLogicalPlans.convert(lPlan), 1);*/

        System.out.println("Meow");
    }

}
