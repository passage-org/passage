package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProjectTest {

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void bgp_of_1_tp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT ?p WHERE {?p <http://address> ?c}";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Bob, Alice, and Carol.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                Arrays.asList("Bob", null),
                Arrays.asList("Alice", null),
                Arrays.asList("Carol", null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void bgp_of_2_tps_project_on_p (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT ?p WHERE {
                 ?p <http://address> <http://nantes> .
                 ?p <http://own> ?a .
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsResultTimes(results, List.of("p", "a"),
                Arrays.asList("Alice", null), 3));
        blazegraph.close();
    }


    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void bgp_of_2_tps_project_on_a (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT ?a WHERE {
                 ?p <http://address> <http://nantes> .
                 ?p <http://own> ?a .
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // dog, snake and cat.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("a", "p"),
                Arrays.asList("dog", null),
                Arrays.asList("snake", null),
                Arrays.asList("cat", null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void bgp_of_2_tps_project_on_both (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT ?p ?a WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // both at once, similar to *
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("a", "p"),
                Arrays.asList("dog", "Alice"),
                Arrays.asList("snake", "Alice"),
                Arrays.asList("cat", "Alice")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void a_project_with_function_in_it (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT ?a (lang(?l) AS ?lang)  WHERE {
                ?a <http://labeled> ?l
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("a", "lang"),
                Arrays.asList("cat", "en")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void a_project_with_function_in_it_with_null_remains_empty (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT ?person (str(?a) AS ?animal)  WHERE {
                ?person <http://address> ?address
                OPTIONAL { ?person <http://own> ?a }
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(5, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                Arrays.asList("Alice", "dog"),
                Arrays.asList("Alice", "cat"),
                Arrays.asList("Alice", "snake"),
                Arrays.asList("Bob", null),
                Arrays.asList("Carol", null)));
        blazegraph.close();
    }

}
