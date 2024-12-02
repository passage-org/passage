package fr.gdd.passage.volcano.pull.pause;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These are not timeout test per se. We emulate timeout with a limit in number of scans.
 * Therefore, the execution can stop in the middle of the execution physical plan. Yet,
 * we must be able to resume execution from where it stopped.
 */
public class PauseOptionalTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(PauseOptionalTimeoutTest.class);

    @BeforeEach
    public void stop_every_scan() { PassageScan.stopping = PauseUtils4Test.stopAtEveryScan; }

    @Test
    public void create_a_bgp_query_and_pause_at_each_scan() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?l .
                OPTIONAL {?p <http://own> ?a .}
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        assertEquals(5, results.size()); // (Alice+animal)*3 + Bob + Carol
        log.debug("{}", results);
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "l", "a"),
                List.of("Alice", "nantes", "cat"),
                List.of("Alice", "nantes", "dog"),
                List.of("Alice", "nantes", "snake"),
                Arrays.asList("Bob", "paris", null),
                Arrays.asList("Carol", "nantes", null)));
    }

    @Test
    public void tp_with_optional_tp_reverse_order () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        log.debug("{}", results);
        assertEquals(3, results.size()); // (Alice * 3)
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                List.of("Alice", "dog"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
    }

    @Test
    public void intermediate_query_that_should_return_one_triple () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE {
                  { SELECT * WHERE { ?person  <http://own>  ?animal } OFFSET 2 }
                  OPTIONAL { ?person  <http://address>  <http://nantes> }
                }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        log.debug("{}", results);
        assertEquals(1, results.size()); // (Alice owns snake)
        assertTrue(MultisetResultChecking.containsResult(results,List.of("person", "animal"),
                List.of("Alice", "dog")) ||
                MultisetResultChecking.containsResult(results,List.of("person", "animal"),
                        List.of("Alice", "cat")) ||
                MultisetResultChecking.containsResult(results,List.of("person", "animal"),
                        List.of("Alice", "snake")));
    }

    @Test
    public void bgp_of_3_tps_and_optional () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        log.debug("{}", results);
        assertEquals(5, results.size()); // (Alice + animal) * 3 + Bob + Carol
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("person", "address", "animal", "specie"),
                List.of("Alice", "nantes", "cat", "feline"),
                List.of("Alice", "nantes", "dog", "canine"),
                List.of("Alice", "nantes", "snake", "reptile"),
                Arrays.asList("Bob", "paris", null, null),
                Arrays.asList("Carol", "nantes", null, null)));
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

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        log.debug("{}", results);
        assertEquals(5, results.size()); // (Alice + animal) * 3 + Bob + Carol
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("person", "address", "animal", "specie"),
                List.of("Alice", "nantes", "cat", "feline"),
                List.of("Alice", "nantes", "dog", "canine"),
                List.of("Alice", "nantes", "snake", "reptile"),
                Arrays.asList("Bob", "paris", null, null),
                Arrays.asList("Carol", "nantes", null, null)));
    }

    @Disabled("Not really a meaningful test.")
    @Test
    public void intermediate_query_should_run () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE { {
                    BIND(<http://Alice> AS ?person)
                    BIND(<http://nantes> AS ?address)
                    OPTIONAL { {
                        SELECT ?animal ?specie WHERE {
                            SELECT ?animal ?specie WHERE { {
                                BIND(<http://Alice> AS ?person)
                                BIND(<http://nantes> AS ?address)
                                BIND(<http://cat> AS ?animal)
                                OPTIONAL { {
                                    SELECT ?specie WHERE {
                                        SELECT * WHERE {
                                            BIND(<http://Alice> AS ?person)
                                            BIND(<http://nantes> AS ?address)
                                            BIND(<http://cat> AS ?animal)
                                            ?animal  <http://species>  ?specie
                                        } OFFSET  0 } } } }
                            UNION { {
                                SELECT * WHERE {
                                    BIND(<http://Alice> AS ?person)
                                    BIND(<http://nantes> AS ?address)
                                    ?person  <http://own>  ?animal
                                } OFFSET  1 }
                                OPTIONAL {
                                    ?animal  <http://species>  ?specie
                                } } } } } } } }
                """;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum); // (Alice + animal) * 3 with every variable setup
    }


}
