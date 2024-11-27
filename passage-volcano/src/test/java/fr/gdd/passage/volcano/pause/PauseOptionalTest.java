package fr.gdd.passage.volcano.pause;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PauseOptionalTest {

    static final Logger log = LoggerFactory.getLogger(PauseOptionalTest.class);
    static final Integer LIMIT = 1;

    @Test
    public void tp_with_optional_tp () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results, LIMIT);
        }
        log.debug("{}", results);
        assertEquals(5, results.size()); // (Alice + animal) * 3 + Bob + Carol
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("person", "address", "animal"),
                List.of("Alice", "nantes", "cat"),
                List.of("Alice", "nantes", "dog"),
                List.of("Alice", "nantes", "snake"),
                Arrays.asList("Bob", "paris", null),
                Arrays.asList("Carol", "nantes", null)));
    }

    @Test
    public void tp_with_optional_tp_reverse_order () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";


        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results, LIMIT);
        }
        log.debug("{}", results);
        assertEquals(3, results.size()); // (Alice * 3)
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("person", "animal"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));

    }

    @Test
    public void bgp_of_3_tps_and_optional () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
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
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, blazegraph, results, LIMIT);
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
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
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

    @Disabled("Not truly a test.")
    @Test
    public void query_with_optional_where_project_filter_too_much () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE
                { { BIND(<http://Alice> AS ?person)
                    BIND(<http://nantes> AS ?address)
                    OPTIONAL { {
                        SELECT  ?animal ?specie WHERE {
                            SELECT  ?animal ?specie WHERE {
                              { BIND(<http://Alice> AS ?person)
                                BIND(<http://nantes> AS ?address)
                                BIND(<http://cat> AS ?animal)
                                OPTIONAL { {
                                    SELECT ?specie WHERE {
                                        SELECT * WHERE {
                                            BIND(<http://Alice> AS ?person)
                                            BIND(<http://nantes> AS ?address)
                                            BIND(<http://cat> AS ?animal)
                                            ?animal  <http://species>  ?specie
                                        } OFFSET  0
                } } } }  } }} }} }
                """;

            var expected = blazegraph.executeQuery(queryAsString);
            log.debug(expected.toString());

            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            // should return alice cat feline, and not feline all alone…
    }

}
