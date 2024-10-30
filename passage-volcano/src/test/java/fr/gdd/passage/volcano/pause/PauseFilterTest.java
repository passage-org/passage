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

public class PauseFilterTest {

    private final static Logger log = LoggerFactory.getLogger(PauseFilterTest.class);

    @BeforeEach
    public void stop_every_scan() {
        PassageScan.stopping = PauseUtils4Test.stopAtEveryScan;
    }

    @Test
    public void simple_values_with_pause () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( ?address != <http://nantes> )
        }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbContinuations = -1;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            ++nbContinuations;
        }
        assertEquals(1, results.size()); // Bob
        assertEquals(2, nbContinuations); // 2 continuations, 3 queries total
        assertTrue(OpExecutorUtils.containsResult(results, List.of("person", "address"),
                List.of("Bob", "paris")));
    }

    @Test
    public void simple_tp_filtered_by_two_vars () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( (?address != <http://nantes>) || (?person != <http://Alice>) )
        }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbContinuations = -1;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            ++nbContinuations;
        }

        assertEquals(2, results.size()); // Bob and Carol
        assertTrue(nbContinuations > 1);
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("person", "address"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
    }


    @Test
    public void simple_bgp_filtered () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbContinuations = -1;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            ++nbContinuations;
        }

        assertTrue(nbContinuations > 1);
        assertEquals(2, results.size()); // Alice and Alice.
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
    }


    @Test
    public void simple_bgp_filtered_in_the_middle () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                FILTER (?c != <http://nantes>)
                ?p <http://own> ?a
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbContinuations = -1;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            ++nbContinuations;
        }

        assertTrue(nbContinuations > 1);
        assertEquals(0, results.size()); // No one that lives outside nantes has animals
    }

    @Test
    public void simple_bgp_filtered_in_the_middle_but_different_order () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
                ?p <http://address> <http://nantes>
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        int nbContinuations = -1;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results);
            ++nbContinuations;
        }

        assertTrue(nbContinuations > 1);
        assertEquals(2, results.size()); // No one that lives outside nantes has animals
    }

}
