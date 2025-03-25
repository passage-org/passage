package fr.gdd.passage.volcano.optimizers;

import se.liu.ida.hefquin.engine.federation.BRTPFServer;
import se.liu.ida.hefquin.engine.federation.Neo4jServer;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.federation.TPFServer;
import se.liu.ida.hefquin.engine.federation.access.*;
import se.liu.ida.hefquin.engine.federation.access.impl.FederationAccessStatsImpl;

import java.util.concurrent.CompletableFuture;

/**
 * If not implemented and called, throw the FederationAccessException by default. It avoids
 * having to implement all the method when not interested. Also, stats are initialized with 0
 * for every kind of request.
 */
public class FederationAccessManagerThrow implements FederationAccessManager {

    protected FederationAccessStatsImpl stats = new FederationAccessStatsImpl(0,0,0,0,0,0,0,0);

    @Override
    public CompletableFuture<SolMapsResponse> issueRequest(SPARQLRequest req, SPARQLEndpoint fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<TPFResponse> issueRequest(TPFRequest req, TPFServer fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<TPFResponse> issueRequest(TPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<TPFResponse> issueRequest(BRTPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<RecordsResponse> issueRequest(Neo4jRequest req, Neo4jServer fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(SPARQLRequest req, SPARQLEndpoint fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(TPFRequest req, TPFServer fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(TPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public CompletableFuture<CardinalityResponse> issueCardinalityRequest(BRTPFRequest req, BRTPFServer fm) throws FederationAccessException {
        throw new FederationAccessException(req, fm);
    }

    @Override
    public FederationAccessStats getStats() {
        return stats;
    }

    @Override
    public void resetStats() {
        stats = new FederationAccessStatsImpl(0,0,0,0,0,0,0,0);
    }
}
