package fr.gdd.passage.volcano.pull.execute;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OptionalTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void optional_with_not_found_value () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://does_not_exist> ?animal}
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice, Alice, and Alice, and Bob, and Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                Arrays.asList("Alice", null),
                Arrays.asList("Bob", null),
                Arrays.asList("Carol", null)));
    }

    @Test
    public void tp_with_optional_tp() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(5, results.size()); // Alice, Alice, and Alice, and Bob, and Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                List.of("Alice", "cat"), List.of("Alice", "dog"), List.of("Alice", "snake"),
                Arrays.asList("Bob", null),
                Arrays.asList("Carol", null)));
    }

    @Test
    public void tp_with_optional_tp_reverse_order() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                List.of("Alice", "cat"), List.of("Alice", "dog"), List.of("Alice", "snake")));
    }

    @Test
    public void bgp_of_3_tps_and_optional() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(5, results.size()); // same as "<address> OPT <own>" query
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal", "specie"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile"),
                Arrays.asList("Bob", null, null),
                Arrays.asList("Carol", null, null)));
    }

    @Test
    public void bgp_of_3_tps_and_optional_of_optional () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   OPTIONAL {?animal <http://species> ?specie}
                 }
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(5, results.size()); // same as "<address> OPT <own>" query
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal", "specie"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile"),
                Arrays.asList("Bob", null, null),
                Arrays.asList("Carol", null, null)));
    }

}
