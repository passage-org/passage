package fr.gdd.passage.volcano.pause;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PauseQuadTimeoutTest {

    private final static Logger log = LoggerFactory.getLogger(PauseQuadTimeoutTest.class);

    @BeforeEach
    public void stop_every_scan() { PassageScan.stopping = PauseUtils4Test.stopAtEveryScan; }

    @Test
    public void empty_quad_pattern () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = """
            SELECT * WHERE {
                GRAPH <http://Alice> { ?p <http://not_known> ?c }
            }
        """;

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        assertEquals(0, results.size()); // not found so nothing
    }

    @Test
    public void simple_pause_during_simple_quad_pattern () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = """
            SELECT * WHERE {
                GRAPH <http://Alice> { ?p <http://own> ?a }
            }
        """;

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbPause = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbPause += 1;
        }
        assertEquals(3, results.size()); // Alice
        assertTrue(nbPause > 1);
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
    }

    @Test
    public void multiple_graphs_are_joined_and_paused () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = """
                SELECT * WHERE {
                    GRAPH <http://Alice> {?p <http://own> ?a}.
                    GRAPH <http://Bob> {?a <http://species> ?s}
                }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbPause = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbPause += 1;
        }
        assertTrue(nbPause > 1);
        assertEquals(3, results.size()); // 3x Alice, with different species
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile")));
    }


    @Test
    public void bgp_with_3_tps_that_preempt () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = """
               SELECT * WHERE {
                GRAPH ?g1 {?p <http://own> ?a .}
                GRAPH ?g2 {?p <http://address> <http://nantes> .}
                GRAPH ?g3 {?a <http://species> ?s}
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbPause = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbPause += 1;
        }
        assertTrue(nbPause > 1);
        // 3x Alice, with different species, BUT this time, some tp come from multiple locations.
        // g1 -> Alice; g2 -> Alice, and Carol; g3 -> Bob
        assertEquals(6, results.size());

        assertTrue(MultisetResultChecking.containsResultTimes(results, List.of("p", "a", "s"),
                List.of("Alice", "cat", "feline"), 2));
        assertTrue(MultisetResultChecking.containsResultTimes(results, List.of("p", "a", "s"),
                List.of("Alice", "dog", "canine"), 2));
        assertTrue(MultisetResultChecking.containsResultTimes(results, List.of("p", "a", "s"),
                List.of("Alice", "snake", "reptile"), 2));
    }

}
