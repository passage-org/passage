package fr.gdd.passage.volcano.pull.execute;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LimitTest {

    private final static Logger log = LoggerFactory.getLogger(LimitTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void when_limit_is_0_then_not_results_ofc () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 0";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size()); // nothing
    }

    @Test
    public void very_simple_limit_on_a_small_triple_pattern () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 1";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // either Bob, Alice, or Carol.
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"),
                List.of("Bob", "paris")) ||
                MultisetResultChecking.containsResult(results, List.of("p", "c"),
                        List.of("Alice", "nantes")) ||
                MultisetResultChecking.containsResult(results, List.of("p", "c"),
                        List.of("Carol", "nantes")));
    }

    @Test
    public void overestimated_limit_for_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 42";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // limit 42 but only 3 still
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Bob", "paris"),
                List.of("Alice", "nantes"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void limit_on_a_bgp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://own> ?a } LIMIT 1";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "snake")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "dog")));
    }

    @Test
    public void limit_as_a_nested_subquery () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                { SELECT * WHERE { ?p <http://own> ?a } LIMIT 1 }
            }""";

        // still should get a result no matter what because the only owner
        // Alice has an address.
        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "snake")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "dog")));
    }

    /* ******************************** LIMIT OFFSET *********************************** */

    @Test
    public void limit_offset_on_simple_triple_pattern () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 1";
        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // either Bob, Alice, or Carol.

        queryAsString = "SELECT * WHERE {?p <http://address> ?c} OFFSET 1 LIMIT 1";
        results.addAll(OpExecutorUtils.executeWithPassage(queryAsString, blazegraph));
        assertEquals(2, results.size()); // either Bob, Alice, or Carol.

        queryAsString = "SELECT * WHERE {?p <http://address> ?c} OFFSET 2 LIMIT 1";
        results.addAll(OpExecutorUtils.executeWithPassage(queryAsString, blazegraph));
        assertEquals(3, results.size()); // either Bob, Alice, or Carol.

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Bob", "paris"),
                List.of("Alice", "nantes"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void limit_offset_in_bgp_but_on_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
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
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
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

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
    }

    @Test
    public void should_take_into_account_the_compatibility_of_input () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
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

    @Test
    public void make_sure_that_the_limit_offset_is_not_applies_to_each_tp_in_bgp () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    ?a <http://species> ?s. # nantes
                    {SELECT * WHERE {
                        ?p <http://own> ?a .
                        ?a <http://species> <http://reptile>
                    } LIMIT 1 }
            }""";

        var expected = blazegraph.executeQuery(queryAsString);
        log.debug("Expected: {}", expected);

        PassageExecutionContext ec = new PassageExecutionContextBuilder().setBackend(blazegraph).setForceOrder(true).build();
        var results = OpExecutorUtils.executeWithPassage(queryAsString, ec);
        assertEquals(1, results.size()); // should be 1, (processed multiple times without optimization)
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "a", "s"),
                List.of("Alice", "snake", "reptile")));
    }

    @Test
    public void offset_alone_on_bgp () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    ?p <http://address> ?c .
                    ?p <http://own> ?a
                } OFFSET 2""";

        var expected = blazegraph.executeQuery(queryAsString);
        log.debug("Expected: {}", expected);

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // skipped 2 results, so there is only one left.
    }

    @Test
    public void offset_alone_on_bgp_above_nb_results () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    ?p <http://address> ?c .
                    ?p <http://own> ?a
                } OFFSET 1000""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size()); // skip all
    }

}
