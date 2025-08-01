package fr.gdd.raw.benchmarks;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.raw.RawOpExecutorUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set of tests dedicated to COUNT queries (i.e. without DISTINCT clauses).
 * The dataset is WatDiv with 10M triples. It does not take too much time to
 * ingest, does not have a lot of weird characters, etc. Although it has long
 * strings as object literal sometimes.
 */
@Disabled
public class RawWatdivCountTest {

    private final static Logger log = LoggerFactory.getLogger(RawWatdivCountTest.class);
    private final static String WATDIV_PATH = "/Users/skoazell/Desktop/Projects/datasets/watdiv10m-blaze/watdiv10M.jnl";

    @Disabled
    @Test
    public void count_star_on_spo () throws RepositoryException, SailException {
        final BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WATDIV_PATH);
        String queryAsString = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }";
        RawOpExecutorUtils.execute(queryAsString, watdivBlazegraph, 1L); // 10,916,457 triples
        // the count is exact with blazegraph, do not need anything but getting a cardinality
        // of spo.
        watdivBlazegraph.close();
    }

    @Disabled
    @Test
    public void count_s_on_spo () throws RepositoryException, SailException {
        final BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WATDIV_PATH);
        // TODO TODO TODO variable bound in COUNT
        // TODO same for p, and o
        String queryAsString = "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }";
        RawOpExecutorUtils.execute(queryAsString, watdivBlazegraph, 1L); // 10,916,457 since 10M triples
        watdivBlazegraph.close();
    }

    @Disabled
    @Test
    public void count_distinct_on_2_tps () throws RepositoryException, SailException {
        final BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WATDIV_PATH);
        String bgpCount = """
                SELECT (COUNT(*) AS ?count) WHERE {
                    ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1> .
                    ?v0 <http://xmlns.com/foaf/givenName> ?v1 .
                    ?v0 <http://schema.org/nationality> ?v3 .
                    ?v2 <http://www.geonames.org/ontology#parentCountry> ?v3 .
                    ?v4 <http://schema.org/eligibleRegion> ?v3 .
                }""";
        RawOpExecutorUtils.execute(bgpCount, watdivBlazegraph, 10_000_000L);
        watdivBlazegraph.close();
    }

}
