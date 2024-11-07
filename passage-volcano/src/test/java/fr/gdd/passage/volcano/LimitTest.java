package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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

    /* ******************************** LIMIT OFFSET *********************************** */

    @Test
    public void limit_offset_on_simple_triple_pattern () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 1";
        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // either Bob, Alice, or Carol.

        queryAsString = "SELECT * WHERE {?p <http://address> ?c} OFFSET 1 LIMIT 1";
        results.addAll(OpExecutorUtils.executeWithPassage(queryAsString, blazegraph));
        assertEquals(2, results.size()); // either Bob, Alice, or Carol.

        queryAsString = "SELECT * WHERE {?p <http://address> ?c} OFFSET 2 LIMIT 1";
        results.addAll(OpExecutorUtils.executeWithPassage(queryAsString, blazegraph));
        assertEquals(3, results.size()); // either Bob, Alice, or Carol.

        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "c"),
                List.of("Bob", "paris"),
                List.of("Alice", "nantes"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void limit_offset_in_bgp_but_on_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                {SELECT * WHERE { ?p <http://own> ?a } OFFSET 1 LIMIT 1 }
            }""";
        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // either dog, cat, or snake.
    }

    @Test
    public void limit_offset_on_bgp_should_work_now () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryA = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
            } LIMIT 1
            """;
        var results = OpExecutorUtils.executeWithPassage(queryA, blazegraph);
        assertEquals(1, results.size()); // either dog, cat, or snake.

        String queryB = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
            } OFFSET 1 LIMIT 1
            """;
        results.addAll(OpExecutorUtils.executeWithPassage(queryB, blazegraph));
        assertEquals(2, results.size()); // either dog, cat, or snake.

        String queryC = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
            } OFFSET 2 LIMIT 1
            """;
        results.addAll(OpExecutorUtils.executeWithPassage(queryC, blazegraph));
        assertEquals(3, results.size()); // either dog, cat, or snake.

        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
    }

    @Test
    public void should_take_into_account_the_compatibility_of_input () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
            SELECT * WHERE {
                <http://Bob> <http://address> ?c.
                {SELECT * WHERE {
                    ?p <http://address> ?c .
                    ?p <http://own> ?a
                } OFFSET 1 LIMIT 2}
            }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size()); // should be 0 as Bob lives in Paris, and no one owns animals in Paris
    }

}
