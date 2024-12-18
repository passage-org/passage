package fr.gdd.passage.volcano.executions;

import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LimitOffsetTest {

    private final static Logger log = LoggerFactory.getLogger(LimitOffsetTest.class);

    /* ****************************** LIMIT ******************************** */

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void when_limit_is_0_then_not_results_ofc (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 0";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size()); // nothing
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void simple_limit_offset_on_single_triple_pattern (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(backend);
        String queryAsString = """
               SELECT * WHERE {
                  ?person <http://address> ?city
               } LIMIT 1""";

        Multiset<BackendBindings<?,?>> results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Alice", "nantes")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Bob", "paris")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Carol", "nantes")));
        backend.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void overestimated_limit_for_tp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 42";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // limit 42 but only 3 still
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Bob", "paris"),
                List.of("Alice", "nantes"),
                List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void limit_on_a_bgp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://own> ?a } LIMIT 1";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "snake")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "dog")));
        blazegraph.close();
    }


    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void limit_as_a_nested_subquery (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                { SELECT * WHERE { ?p <http://own> ?a } LIMIT 1 }
            }""";

        // still should get a result no matter what because the only owner
        // Alice has an address.
        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "snake")) ||
                MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                        List.of("Alice", "dog")));
        blazegraph.close();
    }

    /* ********************************** OFFSET ************************************ */

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void offset_alone (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(backend);
        String queryAsString = """
               SELECT * WHERE {
                  ?person <http://address> ?city
               } OFFSET 2""";

        var expected = backend.executeQuery(queryAsString);
        log.debug("Expected: {}", expected);

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Alice", "nantes")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Bob", "paris")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Carol", "nantes")));
        backend.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#justGo")
    public void limit_of_a_limit_of_a_triple_pattern (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(backend);
        String queryAsString = """
               SELECT * WHERE {
                  SELECT * WHERE {?person <http://address> ?city} OFFSET 1
               } OFFSET 1""";

        var expected = backend.executeQuery(queryAsString);
        log.debug("Expected: {}", expected); // [[person=http://Bob;city=http://paris]]

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Alice", "nantes")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Bob", "paris")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Carol", "nantes")));
        backend.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void offset_alone_on_bgp (PassageExecutionContextBuilder<?,?> builder) throws QueryEvaluationException, MalformedQueryException, RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                    ?p <http://address> ?c .
                    ?p <http://own> ?a
                } OFFSET 2""";

        var expected = blazegraph.executeQuery(queryAsString);
        log.debug("Expected: {}", expected);

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // skipped 2 results, so there is only one left.
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void offset_alone_on_bgp_above_nb_results (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                    ?p <http://address> ?c .
                    ?p <http://own> ?a
                } OFFSET 1000""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size()); // skip all
        blazegraph.close();
    }

    /* ******************************** LIMIT OFFSET *********************************** */

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void limit_offset_on_bgp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(backend);
        String queryAsString = """
               SELECT * WHERE {
                  ?person <http://address> ?city .
                  ?person <http://own> ?animal
               } LIMIT 2 OFFSET 1""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        assertEquals(2, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "city", "animal"), List.of("Alice", "nantes", "dog")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city", "animal"), List.of("Alice", "nantes", "snake")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city", "animal"), List.of("Alice", "nantes" ,"cat")));
        backend.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void limit_offset_on_simple_triple_pattern (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 1";
        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // either Bob, Alice, or Carol.

        queryAsString = "SELECT * WHERE {?p <http://address> ?c} OFFSET 1 LIMIT 1";
        results.addAll(ExecutorUtils.execute(queryAsString, builder));
        assertEquals(2, results.size()); // either Bob, Alice, or Carol.

        queryAsString = "SELECT * WHERE {?p <http://address> ?c} OFFSET 2 LIMIT 1";
        results.addAll(ExecutorUtils.execute(queryAsString, builder));
        assertEquals(3, results.size()); // either Bob, Alice, or Carol.

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Bob", "paris"),
                List.of("Alice", "nantes"),
                List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#multiThreadsPush")
    public void limit_offset_in_bgp_but_on_tp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                {SELECT * WHERE { ?p <http://own> ?a } OFFSET 1 LIMIT 1 }
            }""";
        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // either dog, cat, or snake.
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void limit_offset_on_bgp_should_work_now (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryA = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
            } LIMIT 1
            """;
        var results = ExecutorUtils.execute(queryA, builder);
        assertEquals(1, results.size()); // either dog, cat, or snake.

        String queryB = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
            } OFFSET 1 LIMIT 1
            """;
        results.addAll(ExecutorUtils.execute(queryB, builder));
        assertEquals(2, results.size()); // either dog, cat, or snake.

        String queryC = """
            SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
            } OFFSET 2 LIMIT 1
            """;
        results.addAll(ExecutorUtils.execute(queryC, builder));
        assertEquals(3, results.size()); // either dog, cat, or snake.

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void should_take_into_account_the_compatibility_of_input (PassageExecutionContextBuilder<?,?> builder) throws Exception {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
            SELECT * WHERE {
                <http://Bob> <http://address> ?c.
                {SELECT * WHERE {
                    ?p <http://address> ?c .
                    ?p <http://own> ?a
                } OFFSET 1 LIMIT 2}
            }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size()); // should be 0 as Bob lives in Paris, and no one owns animals in Paris
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#multiThreadsPush")
    public void make_sure_that_the_limit_offset_is_not_applies_to_each_tp_in_bgp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, QueryEvaluationException, MalformedQueryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
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


        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // should be 1, (processed multiple times without optimization)
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "a", "s"),
                List.of("Alice", "snake", "reptile")));
        blazegraph.close();
    }


    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void issue_with_two_subqueries_that_are_joined (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        // The offset was applied to both queries, while each subquery should have
        // its own limit offset fields in their context.
        String queryAsString = """
                SELECT * WHERE { {
                    SELECT * WHERE { ?a  <http://species>  ?s } OFFSET  2  }
                    { SELECT * WHERE {
                        ?a  <http://species>  <http://reptile> .
                        ?p  <http://own>  ?a
                    } LIMIT 1 }
                }""";

        var expected = blazegraph.executeQuery(queryAsString);
        log.debug("Expected: {}", expected);

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        blazegraph.close();
    }

}
