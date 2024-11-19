package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProjectTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void bgp_of_1_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT ?p WHERE {?p <http://address> ?c}";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Bob, Alice, and Carol.
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "c"),
                Arrays.asList("Bob", null),
                Arrays.asList("Alice", null),
                Arrays.asList("Carol", null)));
    }

    @Test
    public void bgp_of_2_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT ?p WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(OpExecutorUtils.containsResultTimes(results, List.of("p", "c"),
                Arrays.asList("Alice", null), 3));

        queryAsString = """
               SELECT ?a WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // dog, snake and cat.
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("a", "p"),
                Arrays.asList("dog", null),
                Arrays.asList("snake", null),
                Arrays.asList("cat", null)));

        queryAsString = """
               SELECT ?p ?a WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // both at once, similar to *
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("a", "p"),
                Arrays.asList("dog", "Alice"),
                Arrays.asList("snake", "Alice"),
                Arrays.asList("cat", "Alice")));
    }

}
