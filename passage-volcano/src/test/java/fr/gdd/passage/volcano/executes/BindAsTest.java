package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BindAsTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void create_a_bind_for_nothing () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Someone> AS ?p)
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p"),
                List.of("Someone")));
    }

    @Test
    public void create_a_bind_and_execute_a_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Alice> AS ?p)
                ?p  <http://own>  ?a .
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"), List.of("Alice", "dog"), List.of("Alice", "snake")));
    }

    @Test
    public void a_bind_with_an_expression_to_evaluate () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                BIND (12+30-2*2 AS ?count)
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(1, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("count"),
                List.of("38")));
    }

    @Test
    public void function_in_bind () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        String queryAsString = """
                SELECT ?animal ?number WHERE {
                       ?person <http://own> ?animal
                       BIND (strlen(str(?animal)) AS ?number)
                }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // no snake this time
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal", "number"),
                List.of("snake", "12"),
                List.of("dog", "10"),
                List.of("cat", "10")));

    }
}
