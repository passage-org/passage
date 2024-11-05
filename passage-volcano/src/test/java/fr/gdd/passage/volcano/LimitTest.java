package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LimitTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void when_limit_is_0_then_not_results_ofc () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 0";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size()); // nothing
    }

    @Test
    public void very_simple_limit_on_a_small_triple_pattern () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 1";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // either Bob, Alice, or Carol.
        assertTrue(OpExecutorUtils.containsResult(results, List.of("p", "c"),
                List.of("Bob", "paris")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c"),
                        List.of("Alice", "nantes")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c"),
                        List.of("Carol", "nantes")));
    }

    @Test
    public void overestimated_limit_for_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 42";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // limit 42 but only 3 still
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "c"),
                List.of("Bob", "paris"),
                List.of("Alice", "nantes"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void limit_on_a_bgp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://own> ?a } LIMIT 1";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size());
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat")) ||
                OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "snake")) ||
                OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "dog")));
    }

    @Test
    public void limit_as_a_nested_subquery () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                { SELECT * WHERE { ?p <http://own> ?a } LIMIT 1 }
            }""";

        // still should get a result no matter what because the only owner
        // Alice has an address.
        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size());
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat")) ||
                OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "snake")) ||
                OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "dog")));
    }

}
