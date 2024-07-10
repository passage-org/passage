package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.sager.SagerConstants;
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
public class Pause2SPARQLBGPTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(Pause2SPARQLBGPTimeoutTest.class);
    final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void create_a_simple_query_and_pause_at_each_scan () {
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();

        }
        assertEquals(3, sum);
    }

    @Test
    public void create_a_bgp_query_and_pause_at_each_scan () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();

        }
        assertEquals(3, sum);
    }

    @Test
    public void create_a_3tps_bgp_query_and_pause_at_each_and_every_scan () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
                ?a <http://species> ?s
               }""";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum);
    }

    @Disabled
    @Test
    public void on_watdiv_conjunctive_query_0_every_scan () {
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

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
            var result = Save2SPARQLTest.executeQuery(query0, watdivBlazegraph);
            sum += result.getLeft();
            query0 = result.getRight();
        }
        assertEquals(326, sum);
    }


    @Disabled
    @Test
    public void on_watdiv_conjunctive_query_10124_every_scan () { // /!\ it takes time (19minutes)
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

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
            var result = Save2SPARQLTest.executeQuery(query10124, watdivBlazegraph);
            sum += result.getLeft();
            query10124 = result.getRight();
            // log.debug("progress = {}", result.getRight());
        }
        // took 19 minutes of execution to pass… (while printing every query)
        assertEquals(117, sum);
    }


    @Disabled
    @Test
    public void on_watdiv_conjunctive_query_10124_every_1k_scans () { // way faster, matter of seconds
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
        SagerScan.stopping = (ec) -> {
            return ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 1000; // stop every 1000 scans
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
            log.debug(query10124);
            var result = Save2SPARQLTest.executeQuery(query10124, watdivBlazegraph);
            sum += result.getLeft();
            query10124 = result.getRight();
            // log.debug("progress = {}", result.getRight());
        }
        // took 19 minutes of execution to pass… (while printing every query)
        assertEquals(117, sum);
    }
}
