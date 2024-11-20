package fr.gdd.raw.iterators;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.raw.RawOpExecutorUtils;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RandomQuadTest {

    private static final Logger log = LoggerFactory.getLogger(RandomQuadTest.class);

    @Test
    public void quad_that_does_not_exist () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.graph3());
        String queryAsStringA = "SELECT * WHERE { GRAPH <http://Alice> {?s <http://does_not_exist> ?a } }";

        // must set a timeout or it runs to the infinity, because no scan can be performed
        var results = RawOpExecutorUtils.executeWithRaw(queryAsStringA, backend, 1000L, 2000L);
        log.debug("{}", results);
        assertEquals(0, results.entrySet().size());

        String queryAsStringB = "SELECT * WHERE { GRAPH <http://does_not_exist> {?s <http://own> ?a } }";
        results = RawOpExecutorUtils.executeWithRaw(queryAsStringB, backend, 1000L, 2000L);
        log.debug("{}", results);
        assertEquals(0, results.entrySet().size());
    }

    @Test
    public void quad_in_single_graph () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.graph3());
        String queryAsString = "SELECT * WHERE { GRAPH <http://Alice> {?s <http://own> ?a } }";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertEquals(3, results.entrySet().size());
        assertEquals(1000, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
    }

    @Test
    public void quad_in_single_graph_but_variable () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.graph3());
        String queryAsString = "SELECT * WHERE { GRAPH ?g {?s <http://own> ?a } }";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertEquals(3, results.entrySet().size());
        assertEquals(1000, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("g", "s", "a"),
                List.of("Alice", "Alice", "cat"),
                List.of("Alice", "Alice", "dog"),
                List.of("Alice", "Alice", "snake")));
    }

    @Test
    public void quad_pattern_with_two_tps_but_work_only_with_2_graphs () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.graph3());
        String queryAsString = "SELECT * WHERE { GRAPH ?g {?s <http://own> ?a . ?a <http://species> ?s} }";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L, 1000L);
        log.debug("{}", results);
        assertEquals(0, results.entrySet().size());
    }

    @Test
    public void quad_pattern_with_two_graphs_joined () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.graph3());
        String queryAsString = """
            SELECT * WHERE {
                GRAPH ?gA {?p <http://own> ?a }
                GRAPH ?gB {?a <http://species> ?s} }""";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertEquals(3, results.entrySet().size());
        // all scans pass, so 500 for first tp, 500 for second tp, therefore 500 results
        assertEquals(500, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("gA", "gB", "p", "a", "s"),
                List.of("Alice", "Bob", "Alice", "cat", "feline"),
                List.of("Alice", "Bob", "Alice", "dog", "canine"),
                List.of("Alice", "Bob", "Alice", "snake", "reptile")));
    }

    @Test
    public void quad_pattern_that_may_fail_whp () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(IM4Blazegraph.graph3());
        String queryAsString = """
            SELECT ?p WHERE {
                GRAPH <http://Alice> {?s ?pred ?o}
                GRAPH <http://Carol> {?p <http://address> ?o} }""";
        // i.e. All persons that have the same address as Alice, i.e. nantes

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertEquals(2, results.entrySet().size());
        assertTrue(results.size() < 500); // at least one fails with high probability
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("p"),
                List.of("Alice"),
                List.of("Carol")));
    }

}
