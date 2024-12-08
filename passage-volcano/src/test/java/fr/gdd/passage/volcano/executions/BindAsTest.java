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

public class BindAsTest {

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void create_a_bind_for_nothing (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Someone> AS ?p)
               }""";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p"),
                List.of("Someone")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void create_a_bind_and_execute_a_tp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Alice> AS ?p)
                ?p  <http://own>  ?a .
               }""";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"), List.of("Alice", "dog"), List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void a_bind_with_an_expression_to_evaluate (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                BIND (12+30-2*2 AS ?count)
               }""";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("count"),
                List.of("38")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void function_in_bind (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT ?animal ?number WHERE {
                       ?person <http://own> ?animal
                       BIND (strlen(str(?animal)) AS ?number)
                }""";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // no snake this time
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal", "number"),
                List.of("snake", "12"),
                List.of("dog", "10"),
                List.of("cat", "10")));
        blazegraph.close();
    }

}
