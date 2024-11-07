package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CountTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void simple_count_on_a_single_triple_pattern() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT (COUNT(*) AS ?count) { ?p <http://address> ?c }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // ?count = 3
        assertTrue(OpExecutorUtils.containsResult(results, List.of("count"),
                List.of("3")));
    }

    @Disabled("Not implemented yet.")
    @Test
    public void count_on_a_specific_variable_but_still_tp() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT (COUNT(?p) AS ?count) { ?p <http://address> ?c }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // ?count = 3
        assertTrue(OpExecutorUtils.containsResult(results, List.of("count"),
                List.of("3")));
    }

    @Disabled("The sub-query should be executed all alone.")
    // The sub-query does not project `p`, so it should probably be a carthesian
    // product, unless the variable `p` is projected in the sub-query. However,
    // it requires a `GROUP BY`, which is then difficult to implement for continuations.
    @Test
    public void simple_count_on_a_single_triple_pattern_driven_by_another_one() throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                {SELECT (COUNT(*) AS ?count) { ?p <http://own> ?animal }}
            }""";

        var expected = blazegraph.executeQuery(query);
        // Alice, Bob, and Carol all have a count of 3 because the sub-query is executed as is.

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(3, results.size()); // ?count = 3 for Alice; Bob and Carol have ?count = 0
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "count"),
                List.of("Alice", "3"), List.of("Bob", "0"), List.of("Carol", "0")));
    }

    @Disabled("Group keys not supported yet")
    @Test
    public void count_with_bound_variables_projected_this_time_by_the_count () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                {SELECT ?p (COUNT(*) AS ?count) { ?p <http://own> ?animal } GROUP BY ?p}
            }""";

        var expected = blazegraph.executeQuery(query);
        // Only Alice is returned, with a value of 3 since only it matches in the sub-query.


        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(3, results.size()); // ?count = 3 for Alice; Bob and Carol have ?count = 0
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "count"),
                List.of("Alice", "3"), List.of("Bob", "0"), List.of("Carol", "0")));
    }

}
