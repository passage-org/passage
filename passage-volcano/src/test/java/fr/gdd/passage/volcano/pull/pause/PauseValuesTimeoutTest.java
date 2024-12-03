package fr.gdd.passage.volcano.pull.pause;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.pull.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PauseValuesTimeoutTest {

    private final static Logger log = LoggerFactory.getLogger(PauseValuesTimeoutTest.class);

    @BeforeEach
    public void stop_every_scan() {
        PassageScan.stopping = PauseUtils4Test.stopAtEveryScan;
    }

    @Test
    public void simple_values_with_pause () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> }
                ?p <http://address> ?c
            }
        """;

        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbContinuations += 1;
        }
        assertEquals(1, results.size());
        // assertEquals(0, nbContinuations); // one result and we know we ain't have more
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
    }

    @Test
    public void simple_values_with_multiple_values () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> }
                ?p <http://address> ?c
            }
        """;

        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbContinuations += 1;
        }
        assertEquals(2, results.size());
        assertTrue(nbContinuations > 0); // multiple results so multiple continuation.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris")));
    }

    @Test
    public void values_in_second_position_ie_after_tp () throws RepositoryException {
        // even though in brTPF, values are in front, we still want to be sure that
        // reordering them does not break anything. It could be important for join ordering.
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
            SELECT * WHERE {
                ?p <http://address> ?c
                VALUES ?p { <http://Alice> <http://Bob> }
            }
        """;

        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbContinuations += 1;
        }
        assertEquals(2, results.size());
        assertTrue(nbContinuations > 0); // multiple results so multiple continuation.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris")));
    }

    @Test
    public void caerthesian_product_with_values () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
            SELECT * WHERE {
                ?person <http://address> ?city
                VALUES ?location { <http://France> <http://Europe> }
            }
        """;


        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbContinuations += 1;
        }
        // it pauses only a few times since it only can pause in scans. Therefore, the VALUES is
        // produced in full before stopping again.
        assertTrue(nbContinuations > 0);
        assertEquals(6, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "city", "location"),
                List.of("Alice", "nantes", "France"),
                List.of("Bob", "paris", "France"),
                List.of("Carol", "nantes", "France"),
                List.of("Alice", "nantes", "Europe"),
                List.of("Bob", "paris", "Europe"),
                List.of("Carol", "nantes", "Europe")));
    }
}
