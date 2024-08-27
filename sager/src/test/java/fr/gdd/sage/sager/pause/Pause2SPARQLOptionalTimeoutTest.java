package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.sager.iterators.SagerScan;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * These are not timeout test per se. We emulate timeout with a limit in number of scans.
 * Therefore, the execution can stop in the middle of the execution physical plan. Yet,
 * we must be able to resume execution from where it stopped.
 */
@Disabled
public class Pause2SPARQLOptionalTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(Pause2SPARQLOptionalTimeoutTest.class);

    @Test
    public void create_a_bgp_query_and_pause_at_each_scan() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?l .
                OPTIONAL {?p <http://own> ?a .}
               }""";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(5, sum); // (Alice+animal)*3 + Bob + Carol
    }

    @Test
    public void tp_with_optional_tp_reverse_order () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum); // (Alice * 3)
    }

    @Test
    public void intermediate_query_that_should_return_one_triple () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
                SELECT * WHERE {
                  { SELECT * WHERE { ?person  <http://own>  ?animal } OFFSET 2 }
                  OPTIONAL { ?person  <http://address>  <http://nantes> }
                }""";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(1, sum); // (Alice owns snake)
    }

    @Test
    public void bgp_of_3_tps_and_optional () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(5, sum); // (Alice + animal) * 3 + Bob + Carol
    }

    @Test
    public void bgp_of_3_tps_and_optional_of_optional () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   OPTIONAL {?animal <http://species> ?specie}
                 }
               }""";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(5, sum); // (Alice + animal) * 3 + Bob + Carol
    }

    @Test
    public void intermediate_query () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
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

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum); // (Alice + animal) * 3 with every variable setup
    }


}
