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

public class DistinctTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void basic_trial_to_create_distinct_without_projected_variable() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT DISTINCT * WHERE { ?p <http://address> ?a }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(3, results.size()); // Alice, Carol, and Bob
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "nantes"), List.of("Bob", "paris"), List.of("Carol", "nantes")));
    }

    @Test
    public void basic_trial_to_create_distinct_from_other_implemented_operators() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT DISTINCT ?a WHERE { ?p <http://address> ?a }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(2, results.size()); // Nantes and Paris
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("a", "p"),
                Arrays.asList("nantes", null), Arrays.asList("paris", null)));
    }

    @Test
    public void distinct_of_bgp() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
        SELECT DISTINCT ?address WHERE {
            ?person <http://address> ?address.
            ?person <http://own> ?animal }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // Nantes only, since only Alice has animals
        assertTrue(OpExecutorUtils.containsResult(results, List.of("address"), List.of("nantes")));
    }

    @Test
    public void distinct_of_bgp_rewritten() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
        SELECT DISTINCT ?address WHERE {
            {SELECT DISTINCT ?address ?person WHERE {
                ?person <http://address> ?address .
            }}
            {SELECT DISTINCT ?person WHERE {
                ?person <http://own> ?animal .
            }}
        }""";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // Nantes only, since only Alice has animals
        assertTrue(OpExecutorUtils.containsResult(results, List.of("address", "person"),
                Arrays.asList("nantes", null)));
    }

}
