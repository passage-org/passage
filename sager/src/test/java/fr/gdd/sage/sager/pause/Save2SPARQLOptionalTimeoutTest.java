package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.sager.iterators.SagerScan;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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
public class Save2SPARQLOptionalTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLOptionalTimeoutTest.class);
    final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void create_a_bgp_query_and_pause_at_each_scan () {
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
    public void tp_with_optional_tp_reverse_order () {
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
    public void intermediate_query_that_should_return_one_triple () {
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
    public void bgp_of_3_tps_and_optional () {
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

//
//    @Disabled
//    @Test
//    public void on_watdiv_conjunctive_query_0_every_scan () {
//        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
//        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;
//
//        String query0 = """
//        SELECT * WHERE {
//        ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>.
//        ?v0 <http://purl.org/goodrelations/validThrough> ?v3.
//        ?v0 <http://purl.org/goodrelations/includes> ?v1.
//        ?v1 <http://schema.org/text> ?v6.
//        ?v0 <http://schema.org/eligibleQuantity> ?v4.
//        ?v0 <http://purl.org/goodrelations/price> ?v2.
//        }""";
//
//        int sum = 0;
//        while (Objects.nonNull(query0)) {
//            var result = Save2SPARQLTest.executeQuery(query0, watdivBlazegraph);
//            sum += result.getLeft();
//            query0 = result.getRight();
//        }
//        assertEquals(326, sum);
//    }
//
//
//    @Disabled
//    @Test
//    public void on_watdiv_conjunctive_query_10124_every_scan () { // /!\ it takes time (19minutes)
//        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
//        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;
//
//        String query10124 = """
//                SELECT * WHERE {
//                        ?v1 <http://www.geonames.org/ontology#parentCountry> ?v2.
//                        ?v3 <http://purl.org/ontology/mo/performed_in> ?v1.
//                        ?v0 <http://purl.org/dc/terms/Location> ?v1.
//                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1>.
//                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId> ?v5.
//                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows> ?v0.
//                }
//                """;
//
//        int sum = 0;
//        while (Objects.nonNull(query10124)) {
//            log.debug(query10124);
//            var result = Save2SPARQLTest.executeQuery(query10124, watdivBlazegraph);
//            sum += result.getLeft();
//            query10124 = result.getRight();
//            // log.debug("progress = {}", result.getRight());
//        }
//        // took 19 minutes of execution to pass… (while printing every query)
//        assertEquals(117, sum);
//    }
//
//
//    @Disabled
//    @Test
//    public void on_watdiv_conjunctive_query_10124_every_1k_scans () { // way faster, matter of seconds
//        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
//        SagerScan.stopping = (ec) -> {
//            return ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 1000; // stop every 1000 scans
//        };
//
//
//        String query10124 = """
//                SELECT * WHERE {
//                        ?v1 <http://www.geonames.org/ontology#parentCountry> ?v2.
//                        ?v3 <http://purl.org/ontology/mo/performed_in> ?v1.
//                        ?v0 <http://purl.org/dc/terms/Location> ?v1.
//                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1>.
//                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId> ?v5.
//                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows> ?v0.
//                }
//                """;
//
//        int sum = 0;
//        while (Objects.nonNull(query10124)) {
//            log.debug(query10124);
//            var result = Save2SPARQLTest.executeQuery(query10124, watdivBlazegraph);
//            sum += result.getLeft();
//            query10124 = result.getRight();
//            // log.debug("progress = {}", result.getRight());
//        }
//        // took 19 minutes of execution to pass… (while printing every query)
//        assertEquals(117, sum);
//    }
}
