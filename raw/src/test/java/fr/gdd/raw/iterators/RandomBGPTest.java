package fr.gdd.raw.iterators;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.raw.RawOpExecutorUtils;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RandomBGPTest {

    private static final Logger log = LoggerFactory.getLogger(RandomBGPTest.class);

    @Test
    public void select_all_from_simple_spo () throws RepositoryException { // as per usual
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?s ?p ?o}";
        // Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        // assertEquals(9, results.elementSet().size());

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertTrue(results.elementSet().size() >= 9); // the 9 + blazegraph's own
        assertEquals(1000, results.size());
        // spo contains other default triples… That why we need more than 100L to
        // retrieve expected spo with high probability.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "p", "o"),
                List.of("Alice", "address", "nantes"),
                List.of("Bob", "address", "paris"),
                List.of("Carol", "address", "nantes"),
                List.of("Alice", "own", "cat"),
                List.of("Alice", "own", "dog"),
                List.of("Alice", "own", "snake"),
                List.of("cat", "species", "feline"),
                List.of("dog", "species", "canine"),
                List.of("snake", "species", "reptile")));
    }

    @Test
    public void simple_triple_pattern () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?s <http://address> ?o}";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend , 1000L);
        log.debug("{}", results);
        assertEquals(3, results.elementSet().size());
        assertEquals(1000, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "o"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void simple_bgp() throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?s <http://address> ?c . ?s <http://own> ?a}";
        // Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        // assertEquals(3, results.elementSet().size());

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertEquals(3, results.elementSet().size());
        assertTrue(results.size() < 500); // because random walks may fail when starting by address
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
    }

    @Test
    public void simple_bgp_of_3_tps() throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://own> ?a . ?a <http://species> ?s}";
        // Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        // assertEquals(3, results.elementSet().size());

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertEquals(3, results.elementSet().size());
        assertTrue(results.size() < 500); // not drastically different of 2tps though
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile")));
    }

}
