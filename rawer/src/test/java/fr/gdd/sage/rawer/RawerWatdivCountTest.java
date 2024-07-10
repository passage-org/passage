package fr.gdd.sage.rawer;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set of tests dedicated to COUNT queries (i.e. without DISTINCT clauses).
 * The dataset is WatDiv with 10M triples. It does not take too much time to
 * ingest, does not have a lot of weird characters, etc. Although it has long
 * strings as object literal sometimes.
 */
@Disabled
public class RawerWatdivCountTest {

    private final static Logger log = LoggerFactory.getLogger(RawerWatdivCountTest.class);
    static BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl");

    @Disabled
    @Test
    public void count_star_on_spo () {
        String queryAsString = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, watdivBlazegraph, 1L); // 10,916,457 triples
        // the count is exact with blazegraph, do not need anything but getting a cardinality
        // of spo.
    }

    @Disabled
    @Test
    public void count_s_on_spo () {
        // TODO TODO TODO variable bound in COUNT
        // TODO same for p, and o
        String queryAsString = "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, watdivBlazegraph, 1L); // 10,916,457 since 10M triples
    }

    @Disabled
    @Test
    public void count_distinct_on_2_tps () {
        String twoTPsQuery = """
                SELECT (COUNT(*) AS ?count) WHERE {
                    ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1> .
                    ?v0 <http://xmlns.com/foaf/givenName> ?v1 .
                    ?v0 <http://schema.org/nationality> ?v3 .
                    ?v2 <http://www.geonames.org/ontology#parentCountry> ?v3 .
                    ?v4 <http://schema.org/eligibleRegion> ?v3 .
                }""";
        RawerOpExecutorTest.execute(twoTPsQuery, watdivBlazegraph, 10_000_000L);
    }

}
