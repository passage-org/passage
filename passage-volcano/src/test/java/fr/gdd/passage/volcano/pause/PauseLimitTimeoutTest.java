package fr.gdd.passage.volcano.pause;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.OpExecutorUtils;
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

public class PauseLimitTimeoutTest {

    private final static Logger log = LoggerFactory.getLogger(PauseLimitTimeoutTest.class);

    @BeforeEach
    public void stop_every_scan() { PassageScan.stopping = PauseUtils4Test.stopAtEveryScan; }

    @Test
    public void test_with_pause_on_simple_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c} LIMIT 2";

        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbContinuations += 1;
        }
        assertEquals(2, results.size());
        assertTrue(nbContinuations >= 1);
        assertTrue(OpExecutorUtils.containsResult(results, List.of("p", "c"),
                List.of("Bob", "paris")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c"),
                        List.of("Alice", "nantes")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c"),
                        List.of("Carol", "nantes")));
    }

    @Test
    public void test_with_pause_on_simple_bgp_inside () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://own> ?a } LIMIT 2";

        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbContinuations += 1;
        }
        assertEquals(2, results.size());
        assertTrue(nbContinuations >= 1);
        assertTrue(OpExecutorUtils.containsResult(results, List.of("p", "c", "a"),
                List.of("Alice", "nantes", "dog")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c", "a"),
                        List.of("Alice", "nantes", "cat")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c", "a"),
                        List.of("Alice", "nantes", "snake")));
    }

    @Test
    public void test_with_pause_with_limit_in_the_bgp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    ?p <http://address> ?c .
                    { SELECT * WHERE { ?p <http://own> ?a } LIMIT 2 }
                }""";

        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            nbContinuations += 1;
        }
        assertEquals(2, results.size());
        assertTrue(nbContinuations >= 1);
        assertTrue(OpExecutorUtils.containsResult(results, List.of("p", "c", "a"),
                List.of("Alice", "nantes", "dog")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c", "a"),
                        List.of("Alice", "nantes", "cat")) ||
                OpExecutorUtils.containsResult(results, List.of("p", "c", "a"),
                        List.of("Alice", "nantes", "snake")));
    }

}
