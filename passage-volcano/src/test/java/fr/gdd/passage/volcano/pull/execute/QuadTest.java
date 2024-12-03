package fr.gdd.passage.volcano.pull.execute;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.pull.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuadTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void a_simple_quad_pattern_with_bounded_graph () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = "SELECT * WHERE {GRAPH <http://Alice> {?p <http://address> ?c}}";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // herself
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                Arrays.asList("Alice", "nantes")));
    }

    @Test
    public void a_simple_quad_pattern_with_unknown_graph () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = "SELECT * WHERE {GRAPH <http://David> {?p <http://address> ?c}}";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size()); // graph does not exist, so 0.
    }

    @Test
    public void a_simple_quad_pattern_with_variable_for_graph () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = "SELECT * WHERE {GRAPH ?g {?p <http://address> ?c}}";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(5, results.size()); // 1 Alice, 1 Bob, 3 Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c", "g"),
                List.of("Alice", "nantes", "Alice"),
                List.of("Bob", "paris", "Bob"),
                List.of("Alice", "nantes", "Carol"),
                List.of("Bob", "paris", "Carol"),
                List.of("Carol", "nantes", "Carol")));
    }

    @Test
    public void multiple_graphs_are_joined () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = """
                SELECT * WHERE {
                    GRAPH <http://Alice> {?p <http://own> ?a}.
                    GRAPH <http://Bob> {?a <http://species> ?s}
                }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // 3x Alice, with different species
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile")));
    }

    @Test
    public void a_graph_with_bgp_inside () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = """
                SELECT * WHERE {
                    GRAPH <http://Alice> {
                        ?p <http://address> ?c .
                        ?p <http://own> ?a}
                }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // 3x Alice, with different species
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
    }

}
