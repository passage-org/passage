package fr.gdd.passage.volcano.push.execute;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.push.streams.PassageSplitScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PushBindAsTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageSplitScan.stopping = (e) -> false; }

    @ParameterizedTest
    @ValueSource(ints = {1,2,5,10})
    public void create_a_bind_for_nothing (int maxParallelism) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Someone> AS ?p)
               }""";

        var results = OpExecutorUtils.executeWithPush(queryAsString, blazegraph, maxParallelism);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p"),
                List.of("Someone")));
        blazegraph.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {1,2,5,10})
    public void create_a_bind_and_execute_a_tp (int maxParallelism) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Alice> AS ?p)
                ?p  <http://own>  ?a .
               }""";

        var results = OpExecutorUtils.executeWithPush(queryAsString, blazegraph, maxParallelism);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"), List.of("Alice", "dog"), List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {1,2,5,10})
    public void a_bind_with_an_expression_to_evaluate (int maxParallelism) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                BIND (12+30-2*2 AS ?count)
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("count"),
                List.of("38")));
        blazegraph.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {1,2,5,10})
    public void function_in_bind (int maxParallelism) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        String queryAsString = """
                SELECT ?animal ?number WHERE {
                       ?person <http://own> ?animal
                       BIND (strlen(str(?animal)) AS ?number)
                }""";

        var results = OpExecutorUtils.executeWithPush(queryAsString, blazegraph, maxParallelism);
        assertEquals(3, results.size()); // no snake this time
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal", "number"),
                List.of("snake", "12"),
                List.of("dog", "10"),
                List.of("cat", "10")));
        blazegraph.close();
    }

}
