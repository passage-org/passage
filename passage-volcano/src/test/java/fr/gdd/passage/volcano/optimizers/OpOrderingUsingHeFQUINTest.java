package fr.gdd.passage.volcano.optimizers;

import com.bigdata.rdf.sparql.ast.optimizers.ASTCardinalityOptimizer;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendManager;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import se.liu.ida.hefquin.base.data.VocabularyMapping;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.access.DataRetrievalInterface;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessManager;
import se.liu.ida.hefquin.engine.federation.access.impl.iface.SPARQLEndpointInterfaceImpl;
import se.liu.ida.hefquin.engine.federation.catalog.FederationCatalog;
import se.liu.ida.hefquin.engine.federation.catalog.impl.FederationCatalogImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;
import se.liu.ida.hefquin.engine.queryproc.LogicalOptimizationException;
import se.liu.ida.hefquin.engine.queryproc.PhysicalOptimizationException;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanningException;
import se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.CardinalityBasedJoinOrderingBase;
import se.liu.ida.hefquin.engine.queryproc.impl.loptimizer.heuristics.CardinalityBasedJoinOrderingWithRequests;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.cardinality.CardinalityEstimationImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.costmodel.CostModelImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.costmodel.MyCostModel;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.EquilibriumConditionByRelativeSubplanCount;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.SimulatedAnnealing;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.StoppingConditionByNumberOfGenerations;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.randomized.TwoPhaseQueryOptimizer;

import java.util.concurrent.ExecutorService;

public class OpOrderingUsingHeFQUINTest {

    @Disabled
    @Test
    public void start_testing_an_approach_to_support_hefquins_optimizers () throws SourcePlanningException, PhysicalOptimizationException, LogicalOptimizationException {
        // TODO have a logical to physical converted.
        FederationCatalogImpl federationCatalog = new FederationCatalogImpl();

        federationCatalog.addMember("http://meow", new FederationMember() {
            @Override public DataRetrievalInterface getInterface() {return new SPARQLEndpointInterfaceImpl("http://meow");}
            @Override public VocabularyMapping getVocabularyMapping() {return null;}
        });

        BackendManager backendManager = new BackendManager();
        backendManager.addBackend("http://meow", (path) -> {
            try {
                return (Backend) new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });

        FederationAccessManager federationManager = new FederationAccessManagerLocal(backendManager);

        ExecutionContext context = new ExecutionContext() {
            @Override public FederationAccessManager getFederationAccessMgr() { return federationManager; }
            @Override public FederationCatalog getFederationCatalog() { return federationCatalog; }
            @Override public ExecutorService getExecutorServiceForPlanTasks() { return null; }
            @Override public boolean isExperimentRun() { return false; }
            @Override public boolean skipExecution() { return false; } };

        Op query = Algebra.compile(QueryFactory.create("SELECT * WHERE  { ?s ?p ?o }"));
        LogicalPlan lPlan = Jena2HeFQUIN.convert(query);

        // vvvv from ExempleEngineConf.ttl
        StoppingConditionByNumberOfGenerations condition1 = new StoppingConditionByNumberOfGenerations(5);
        // vvvv from ExampleEngineConf.ttl as well
        EquilibriumConditionByRelativeSubplanCount condition2 = new EquilibriumConditionByRelativeSubplanCount(16);
        TwoPhaseQueryOptimizer twoPhaseQueryOpt = new TwoPhaseQueryOptimizer(condition1, condition2,
                /* logical2physicalplanconverter */ new Logical2PassagePlan(),
                /* costmodel */  new CostModelImpl(new CardinalityEstimationImpl(context)),
                /* rulesinstance */ new SPARQLRuleInstances() // TODO implements the rules
        );

        // var pair = twoPhaseQueryOpt.optimize(lPlan);

        SimulatedAnnealing annealingOpt = new SimulatedAnnealing(condition2,
                new Logical2PassagePlan(),
                new MyCostModel(new LocalCardinalityEstimationImpl((FederationAccessManagerLocal) federationManager)),
                new SPARQLRuleInstances()
                );

        var pair = annealingOpt.optimize(lPlan);

        CardinalityBasedJoinOrderingBase cardinalityOpt = new CardinalityBasedJoinOrderingWithRequests(context);
        // cardinalityOpt.apply(lPlan);

        System.out.println("Meow");
    }

}
