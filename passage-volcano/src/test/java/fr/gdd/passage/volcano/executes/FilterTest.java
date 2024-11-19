package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
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
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( ?address != <http://nantes> )
        }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // Bob only
        assertTrue(OpExecutorUtils.containsResult(results, List.of("person", "address"),
                List.of("Bob", "paris")));
    }

    @Test
    public void simple_tp_filtered_by_two_vars () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( (?address != <http://nantes>) || (?person != <http://Alice>) )
        }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(2, results.size()); // Bob and Carol
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("person", "address"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void filter_bgp_of_2_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(2, results.size()); // Alice and Alice.
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
    }

}
