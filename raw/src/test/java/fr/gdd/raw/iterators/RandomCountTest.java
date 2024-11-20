package fr.gdd.raw.iterators;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.raw.RawOpExecutorUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RandomCountTest {

    private static final Logger log = LoggerFactory.getLogger(RandomCountTest.class);

    @Test
    public void count_of_simple_triple_pattern () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT (COUNT(*) AS ?c) WHERE {<http://Alice> ?p ?o}";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1L);
        log.debug("{}", results);
        // because blazegraph enable exact count on triple pattern, this only need 1 walk
        // to get 100% accuracy.
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("c"), List.of("4")));
    }

    @Test
    public void count_of_carthesian_product_bgp () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
            SELECT (COUNT(*) AS ?c) WHERE {
                <http://Alice> ?p ?o .
                <http://Alice> <http://own> ?a }""";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1L); // 12 since cartesian product
        log.debug("{}", results);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("c"), List.of("12")));
    }

    @Test
    public void count_of_bgp () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
            SELECT (COUNT(*) AS ?c) WHERE {
                ?person <http://own> ?animal .
                ?person <http://address> ?location }""";

        // join order will fix <own> before <address>, so every RW succeed, so the evaluation is 100% accurate here
        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1L);
        log.debug("{}", results);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("c"), List.of("3")));
    }

    @Test
    public void count_of_bgp_that_may_fail () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT (COUNT(*) AS ?c) WHERE {?person <http://address> ?location . ?person <http://own> ?animal}";

        // ~3 since only Alice has animals. Choosing a person that has no animal
        // makes the RW fails, hence the approximate value.
        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 10000L);
        log.debug("{}", results);
        assertEquals(1, results.size());
        // we check 2 or 3 or 4, since it should be a close value.
        assertTrue(MultisetResultChecking.containsResult(results, List.of("c"), List.of("3.")) ||
                        MultisetResultChecking.containsResult(results, List.of("c"), List.of("2.")) ||
                MultisetResultChecking.containsResult(results, List.of("c"), List.of("4."))
                );
    }

    @Disabled("Not handle yet, should thrown until handled properly…")
    @Test
    public void count_with_group_on_simple_tp () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT ?p (COUNT(*) AS ?c) ?p WHERE {?s ?p ?o} GROUP BY ?p";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 10000L);
        log.debug("{}", results);
        assertTrue(results.size() > 3); // address, own, species + blazegraphs' specials
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p"),
                List.of("address"), List.of("own"), List.of("species")));
    }

}
