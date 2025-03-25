package fr.gdd.passage.volcano.optimizers;

import fr.gdd.passage.commons.generics.BackendManager;
import fr.gdd.passage.commons.interfaces.Backend;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.federation.access.CardinalityResponse;
import se.liu.ida.hefquin.engine.federation.access.DataRetrievalRequest;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessException;
import se.liu.ida.hefquin.engine.federation.access.SPARQLRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.response.CardinalityResponseImpl;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * We do not actually perform http requests. We make calls to local query engines
 * that can handle a subset of SPARQL.
 */
public class FederationAccessManagerLocal extends FederationAccessManagerThrow {

    BackendManager manager;

    FederationAccessManagerLocal(BackendManager manager) {
        this.manager = manager;
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(SPARQLRequest req, SPARQLEndpoint fm) throws FederationAccessException {
        // TODO increment number of requests issued.
        // TODO register the endpoint in the backend manager ?
        Backend<?,?> backend = manager.getBackend(fm.toString());
        // TODO create an engine that performs cardinality estimations, but for now it will be passage

        return CompletableFuture.supplyAsync(() -> {
            final Date start = new Date();
            final int cardinality = 0; // TODO actually execute the request
            final Date stop = new Date();

            CardinalityResponse cr =  new CardinalityResponse() {
                @Override public int getCardinality() {return cardinality;}
                @Override public FederationMember getFederationMember() {return fm;}
                @Override public DataRetrievalRequest getRequest() {return req;}
                @Override public Date getRequestStartTime() {return start;}
                @Override public Date getRetrievalEndTime() {return stop;}
            };
            return cr;
        });
    }
}
