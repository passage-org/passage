package fr.gdd.passage.volcano.pull.pause;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.benchmarks.WatDivTest;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These are not timeout test per se. We emulate timeout with a limit in number of scans.
 * Therefore, the execution can stop in the middle of the execution physical plan. Yet,
 * we must be able to resume execution from where it stopped.
 */
public class PauseBGPTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(PauseBGPTimeoutTest.class);

    @BeforeEach
    public void stop_every_scan() { PassageScan.stopping = PauseUtils4Test.stopAtEveryScan; }

    @Test
    public void create_a_simple_query_and_pause_at_each_scan () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void create_a_bgp_query_and_pause_at_each_scan () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake"),
                List.of("Alice", "dog")));
    }

    @Test
    public void bgp_that_was_a_problem_with_quads () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        String queryAsString = """
                SELECT * WHERE {
                    ?p <http://own> ?a.
                    ?a <http://species> ?s
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
    public void create_a_3tps_bgp_query_and_pause_at_each_and_every_scan () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
                ?a <http://species> ?s
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
        }
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "snake", "reptile")));
    }

    @Disabled("Possibly time consuming.")
    @Test
    public void on_watdiv_conjunctive_query_0_every_scan () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WatDivTest.PATH);

        String query0 = """
        SELECT * WHERE {
        ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>.
        ?v0 <http://purl.org/goodrelations/validThrough> ?v3.
        ?v0 <http://purl.org/goodrelations/includes> ?v1.
        ?v1 <http://schema.org/text> ?v6.
        ?v0 <http://schema.org/eligibleQuantity> ?v4.
        ?v0 <http://purl.org/goodrelations/price> ?v2.
        }""";

        int sum = 0;
        while (Objects.nonNull(query0)) {
            var result = PauseUtils4Test.executeQuery(query0, watdivBlazegraph);
            sum += result.getLeft();
            query0 = result.getRight();
        }
        assertEquals(326, sum);
    }


    @Disabled("Time consuming.")
    @Test
    public void on_watdiv_conjunctive_query_10124_every_scan () throws RepositoryException, SailException { // /!\ it takes time (19minutes)
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WatDivTest.PATH);

        String query10124 = """
                SELECT * WHERE {
                        ?v1 <http://www.geonames.org/ontology#parentCountry> ?v2.
                        ?v3 <http://purl.org/ontology/mo/performed_in> ?v1.
                        ?v0 <http://purl.org/dc/terms/Location> ?v1.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1>.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId> ?v5.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows> ?v0.
                }
                """;

        int sum = 0;
        while (Objects.nonNull(query10124)) {
            log.debug(query10124);
            var result = PauseUtils4Test.executeQuery(query10124, watdivBlazegraph);
            sum += result.getLeft();
            query10124 = result.getRight();
            // log.debug("progress = {}", result.getRight());
        }
        // took 19 minutes of execution to pass… (while printing every query)
        assertEquals(117, sum);
    }


    @Test
    public void on_watdiv_conjunctive_query_10124_every_1k_scans () throws RepositoryException, SailException { // way faster, matter of seconds
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WatDivTest.PATH);
        PassageScan.stopping = (ec) -> {
            return ((AtomicLong) ec.getContext().get(PassageConstants.SCANS)).get() >= 1000L; // stop every 1000 scans
        };

        String query10124 = """
                SELECT * WHERE {
                        ?v1 <http://www.geonames.org/ontology#parentCountry> ?v2.
                        ?v3 <http://purl.org/ontology/mo/performed_in> ?v1.
                        ?v0 <http://purl.org/dc/terms/Location> ?v1.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1>.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId> ?v5.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows> ?v0.
                }
                """;

        int sum = 0;
        while (Objects.nonNull(query10124)) {
            // log.debug(query10124);
            var result = PauseUtils4Test.executeQuery(query10124, watdivBlazegraph);
            sum += result.getLeft();
            query10124 = result.getRight();
            // log.debug("progress = {}", result.getRight());
        }
        // took 19 minutes of execution to pass… (while printing every query)
        assertEquals(117, sum);
    }
}
