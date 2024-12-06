package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValuesTest {

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void undef_in_values (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = "SELECT * WHERE {  VALUES ?p { UNDEF } }";

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(1, results.size()); // 1 result but empty. (Wikidata's online server confirms)
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void values_with_a_term_that_does_not_exist_in_database (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = "SELECT * WHERE {  VALUES ?p { <http://does_not_exist> } }";

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(1, results.size()); // the one that does not exist
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p"), List.of("does_not_exist")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void values_with_a_term_that_does_not_exist_put_in_tp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
                SELECT * WHERE {
                    VALUES ?p { <http://does_not_exist> }
                    ?p <http://address> ?c
                }""";

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(0, results.size()); // does not exist
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void simple_values_with_nothing_else (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = "SELECT * WHERE { VALUES ?p { <http://Alice> }  }";

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(1, results.size()); // Alice lives in Nantes
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p"), List.of("Alice")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void simple_values_with_only_one_variable_value (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(1, results.size()); // Alice lives in Nantes
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void simple_values_with_only_one_variable_but_multiple_values (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(2, results.size()); // Alice lives in Nantes; Bob lives in Paris
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Bob", "paris")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider") // TODO TODO TODO don't pass yet
    public void three_values_in_values (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> <http://Carol>}
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(3, results.size()); // Alice lives in Nantes; Bob lives in Paris
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Bob", "paris")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void values_that_do_not_exist (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> <http://NO_ONE>}
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(2, results.size()); // Alice lives in Nantes; Bob lives in Paris
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Bob", "paris")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void an_empty_values(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
            SELECT * WHERE {
                VALUES ?p { }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(0, results.size()); // Nothing
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void value_in_the_middle (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
            SELECT * WHERE {
                ?p2 <http://address> ?c
                VALUES ?p { <http://Alice> }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(2, results.size()); // (Alice and herself + Alice and Carol) live in nantes
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "p2", "c"), List.of("Alice", "Alice", "nantes")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "p2", "c"), List.of("Alice", "Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void carthesian_product_with_values (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
            SELECT * WHERE {
                ?person <http://address> ?city
                VALUES ?location { <http://France> <http://Europe> }
            }
        """;

        var results = OpExecutorUtils.executeWithPush(query, builder);
        assertEquals(6, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "city", "location"),
                List.of("Alice", "nantes", "France"),
                List.of("Bob", "paris", "France"),
                List.of("Carol", "nantes", "France"),
                List.of("Alice", "nantes", "Europe"),
                List.of("Bob", "paris", "Europe"),
                List.of("Carol", "nantes", "Europe")));
        blazegraph.close();
    }

}
