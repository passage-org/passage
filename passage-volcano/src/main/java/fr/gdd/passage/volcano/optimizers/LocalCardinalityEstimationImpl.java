package fr.gdd.passage.volcano.optimizers;

import se.liu.ida.hefquin.base.data.VocabularyMapping;
import se.liu.ida.hefquin.base.query.SPARQLQuery;
import se.liu.ida.hefquin.base.query.impl.SPARQLQueryImpl;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.federation.access.DataRetrievalInterface;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessException;
import se.liu.ida.hefquin.engine.federation.access.SPARQLEndpointInterface;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.iface.SPARQLEndpointInterfaceImpl;
import se.liu.ida.hefquin.engine.federation.access.impl.req.SPARQLRequestImpl;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.CardinalityEstimation;

import java.util.concurrent.CompletableFuture;

public class LocalCardinalityEstimationImpl implements CardinalityEstimation {

    final FederationAccessManagerLocal manager;

    public LocalCardinalityEstimationImpl(FederationAccessManagerLocal manager) {
        this.manager = manager;
    }

    @Override
    public CompletableFuture<Integer> initiateCardinalityEstimation(PhysicalPlan plan) {
        FederationMember fm = new FederationMember() { // TODO change this
            @Override public DataRetrievalInterface getInterface() {return new SPARQLEndpointInterfaceImpl("http://meow");}
            @Override public VocabularyMapping getVocabularyMapping() {return null;}
        };
        SPARQLEndpoint endpoint = new SPARQLEndpoint() {
            @Override public SPARQLEndpointInterface getInterface() {return new SPARQLEndpointInterfaceImpl("http://meow");}

            @Override public VocabularyMapping getVocabularyMapping() {return null;}
        };

        if (plan.getRootOperator() instanceof SPARQLRequest sprql) {
            try {
                return manager.issueCardinalityRequest(sprql, endpoint).thenApply((m) -> m.getCardinality());
            } catch (FederationAccessException e) {
                throw new RuntimeException(e);
            }
        }
        throw new UnsupportedOperationException(plan.getClass().getCanonicalName());
    }

}
