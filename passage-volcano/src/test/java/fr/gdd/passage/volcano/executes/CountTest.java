package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CountTest {

    private final static Logger log = LoggerFactory.getLogger(CountTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void simple_count_on_a_single_triple_pattern() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = "SELECT (COUNT(*) AS ?count) { ?p <http://address> ?c }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // ?count = 3
        assertTrue(MultisetResultChecking.containsResult(results, List.of("count"),
                List.of("3")));
    }

    @Test
    public void count_of_something_that_does_not_exist_is_zero() throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = "SELECT (COUNT(*) AS ?count) { ?p <http://does_not_exist> ?c }";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // not even a ?count = 0
        assertTrue(MultisetResultChecking.containsResult(results,
                List.of("count"),
                List.of("0")));
    }

    @Test
    public void variable_undefined_in_subquery () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = "SELECT (COUNT(?undefined) AS ?count) { ?p <http://address> ?c }";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("count"), List.of("0")));
    }

    @Test
    public void count_in_bgp_without_results () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = """
            SELECT (COUNT(*) AS ?count) {
                VALUES ?c { <http://washington> }
                ?p <http://address> ?c.
            }""";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("count"), List.of("0")));
    }


    @Test
    public void count_on_a_specific_variable_but_still_tp() throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = "SELECT (COUNT(?p) AS ?count) { ?p <http://address> ?c }";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // ?count = 3
        assertTrue(MultisetResultChecking.containsResult(results, List.of("count"),
                List.of("3")));
    }

    @Test
    public void count_in_optional_so_everything_does_not_count() throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = """
            SELECT (COUNT(?a) AS ?count) {
                ?p <http://address> ?c
                OPTIONAL { ?p <http://own> ?a }}""";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // ?count = 3
        assertTrue(MultisetResultChecking.containsResult(results, List.of("count"),
                List.of("3"))); // if count=5, it means that the iterator wrongfully counted Bob and Carol…
    }

    @Test
    public void a_tp_join_with_a_count_subquery() throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        // The sub-query does not project `p`, so it should probably be a carthesian
        // product, unless the variable `p` is projected in the sub-query. However,
        // it requires a `GROUP BY`, which is then difficult to implement for continuations.
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                {SELECT (COUNT(*) AS ?count) { ?p <http://own> ?animal }}
            }""";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);
        // Alice, Bob, and Carol all have a count of 3 because the sub-query is executed as is.

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "count"),
                List.of("Alice", "3"), List.of("Bob", "3"), List.of("Carol", "3")));
    }

    @Test
    public void count_with_bound_variables_projected_this_time_by_the_count () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                {SELECT ?p (COUNT(*) AS ?count) { ?p <http://own> ?animal } GROUP BY ?p}
            }""";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);
        // Only Alice is returned, with a value of 3 since only it matches in the sub-query.

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // ?count = 3 for Alice; Bob and Carol don't even exist.
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "count"), List.of("Alice", "3")));
    }

    @Test
    public void count_with_group_by_at_the_top () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        // For now, it throws because GROUP BY needs DISTINCT on keys when they are not bounded by the environment.
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = """
                SELECT ?p (COUNT(*) AS ?nbAnimals) WHERE {
                    ?p <http://address> ?c .
                    ?p <http://own> ?animal
                } GROUP BY ?p""";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        assertThrows(UnsupportedOperationException.class, () -> OpExecutorUtils.executeWithPassage(query, blazegraph));
    }

    @Disabled("Not implemented yet when multiple COUNTs in a single (sub-)query.")
    @Test
    public void multiple_counts_in_a_single_aggregate () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = """
            SELECT (COUNT(?p) AS ?pCount) (COUNT(?animal) AS ?aCount) WHERE {
                ?p <http://own> ?animal
            }""";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results,
                List.of("pCount", "aCount"),
                List.of("3", "3"))); // both 3 since they count the same
    }

    @Disabled("Not implemented yet when multiple COUNTs in a single (sub-)query.")
    @Test
    public void multiple_counts_in_a_single_aggregate_with_optional () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String query = """
            SELECT (COUNT(?p) AS ?pCount) (COUNT(?a) AS ?aCount) WHERE {
                ?p <http://address> ?c
                OPTIONAL { ?p <http://own> ?a }
            }""";

        var expected = blazegraph.executeQuery(query);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results,
                List.of("pCount", "aCount"),
                List.of("5", "3"))); // p: 3 Alices + Bob + Carol; a: dog + cat + snake
    }

}
