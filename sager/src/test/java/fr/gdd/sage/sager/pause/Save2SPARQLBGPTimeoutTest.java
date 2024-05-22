package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.databases.inmemory.IM4Jena;
import fr.gdd.sage.jena.JenaBackend;
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
public class Save2SPARQLBGPTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLBGPTimeoutTest.class);
    final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void create_a_simple_query_and_pause_at_each_scan () {
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";
        log.debug(queryAsString);

        SagerScan.stopping = (ec) -> {
            return ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 1; // stop at every scan
        };

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
            log.debug(queryAsString);
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

        SagerScan.stopping = (ec) -> {
            return ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 1; // stop at every scan
        };

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
    public void create_a_bgp_query_and_pause_at_each_result_but_different_order () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
               }""";

        SagerScan.stopping = (ec) -> {
            return ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 1; // stop at every scan
        };

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum);
    }

}
