package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FilterTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void simple_tp_filtered_by_one_var () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( ?address != <http://nantes> )
        }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // Bob only
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "address"),
                List.of("Bob", "paris")));
    }

    @Test
    public void simple_tp_filtered_by_two_vars () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( (?address != <http://nantes>) || (?person != <http://Alice>) )
        }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(2, results.size()); // Bob and Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "address"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void filter_bgp_of_2_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(2, results.size()); // Alice and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
    }

    @Test
    public void filter_using_a_literal_integer () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        String queryAsString = """
                SELECT * WHERE {
                       ?animal <http://letters> ?number
                       FILTER (?number > 3)
                }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size());
        // cat = 3 so filtered out
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal", "number"),
                List.of("snake", "5")));
    }

    @Test
    public void filter_using_a_literal_and_a_function () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        String queryAsString = """
                SELECT ?animal WHERE {
                       ?person <http://own> ?animal
                       FILTER (strlen(str(?animal)) <= 8+3)
                }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(2, results.size()); // no snake this time
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal"),
                List.of("dog"),
                List.of("cat")));

    }

}
