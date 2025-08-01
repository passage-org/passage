package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
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

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class OptionalTest {

    private final static Logger log = LoggerFactory.getLogger(OptionalTest.class);

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void optional_with_not_found_value (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://does_not_exist> ?animal}
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Alice, Alice, and Alice, and Bob, and Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                Arrays.asList("Alice", null),
                Arrays.asList("Bob", null),
                Arrays.asList("Carol", null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void tp_with_optional_tp(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(5, results.size()); // Alice, Alice, and Alice, and Bob, and Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                List.of("Alice", "cat"), List.of("Alice", "dog"), List.of("Alice", "snake"),
                Arrays.asList("Bob", null),
                Arrays.asList("Carol", null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void tp_with_optional_tp_reverse_order(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                List.of("Alice", "cat"), List.of("Alice", "dog"), List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void bgp_of_3_tps_and_optional(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(5, results.size()); // same as "<address> OPT <own>" query
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal", "specie"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile"),
                Arrays.asList("Bob", null, null),
                Arrays.asList("Carol", null, null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void bgp_of_3_tps_and_optional_of_optional (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   OPTIONAL {?animal <http://species> ?specie}
                 }
               }""";

        var expected = blazegraph.executeQuery(queryAsString);
        log.debug("{}", expected);

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(5, results.size()); // same as "<address> OPT <own>" query
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal", "specie"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile"),
                Arrays.asList("Bob", null, null),
                Arrays.asList("Carol", null, null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void intermediate_query_that_should_return_one_triple (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                  { SELECT * WHERE { ?person  <http://own>  ?animal } OFFSET 2 }
                  OPTIONAL { ?person  <http://address>  <http://nantes> }
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // (Alice owns snake)
        assertTrue(MultisetResultChecking.containsResult(results,List.of("person", "animal"),
                List.of("Alice", "dog")) ||
                MultisetResultChecking.containsResult(results,List.of("person", "animal"),
                        List.of("Alice", "cat")) ||
                MultisetResultChecking.containsResult(results,List.of("person", "animal"),
                        List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void a_tp_with_then_a_list_of_optionals (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                  ?person <http://address> ?address .
                  OPTIONAL { ?person <http://nothing> ?nothing }
                  OPTIONAL { ?person  <http://own>  ?animal }
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(5, results.size()); // (Everyone with their animal optionally)
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal", "address"),
                List.of("Alice", "cat", "nantes"),
                List.of("Alice", "dog", "nantes"),
                List.of("Alice", "snake", "nantes"),
                Arrays.asList("Bob", null, "paris"),
                Arrays.asList("Carol", null, "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void an_optional_filtered (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                  ?person <http://own> ?animal .
                  OPTIONAL { ?animal  <http://species>  ?species . FILTER (regex(str(?species), "feline")) }
                }""";

        // It used to throw, but not anymore: // TODO throw again to fix
        assertThrows(UnsupportedOperationException.class, () -> ExecutorUtils.execute(queryAsString, builder));

//        var results = ExecutorUtils.execute(queryAsString, builder);
//        assertEquals(3, results.size());
//        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal", "species"),
//                List.of("Alice", "cat", "feline"),
//                Arrays.asList("Alice", "dog", null),
//                Arrays.asList("Alice", "snake", null)));
        blazegraph.close();
    }

    @ParameterizedTest
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void an_optional_filtered_intermediate (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                { SELECT  *  WHERE  { ?person  <http://own>  ?animal } OFFSET  1 }
                  OPTIONAL { {
                      ?animal  <http://species>  ?species
                      FILTER regex(str(?species), "feline") }  }  }
                """;

        // It used to throw, but not anymore:
        // assertThrows(UnsupportedOperationException.class, () -> ExecutorUtils.execute(queryAsString, builder));

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // species are filtered out, but Alice remains
        blazegraph.close();
    }

}
