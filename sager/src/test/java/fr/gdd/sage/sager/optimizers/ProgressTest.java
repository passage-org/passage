package fr.gdd.sage.sager.optimizers;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.sager.pause.Pause2SPARQLBGPTimeoutTest;
import fr.gdd.sage.sager.pause.Save2SPARQLTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
class ProgressTest {

    private static final Logger log = LoggerFactory.getLogger(Pause2SPARQLBGPTimeoutTest.class);
    final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void create_a_simple_query_and_pause_at_each_scan () {
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQueryWithProgress(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getMiddle();
            double progress = result.getRight();

            log.debug("progress = {}", progress);
            assertEquals(sum/3., progress); // hopefully no rounding errors…
        }
    }

    @Test
    public void create_a_bgp_query_and_pause_at_each_result_but_different_order () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
               }""";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQueryWithProgress(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getMiddle();
            log.debug("progress = {}", result.getRight());
        }
        assertEquals(3, sum);
    }

    @Test
    public void bgp_with_3_tps_that_preempt () {
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
                ?a <http://species> ?s
               }""";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQueryWithProgress(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getMiddle();
            log.debug("progress = {}", result.getRight());
        }
        assertEquals(3, sum);
    }


}