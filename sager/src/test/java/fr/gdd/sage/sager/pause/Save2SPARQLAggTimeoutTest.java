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

@Disabled
public class Save2SPARQLAggTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLBGPTimeoutTest.class);
    final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void count_of_tp_but_stops_inside_the_operator () {
        String queryAsString = "SELECT (COUNT(*) AS ?count) { ?p <http://address> ?c }";

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        int nbPreempt = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
            nbPreempt += 1;

        }
        assertEquals(1, sum); // 1 result where ?count = 3
        assertEquals(3, nbPreempt);
    }

    @Test
    public void count_of_tp_but_stops_inside_the_operator_focusing_on_last_step () {
        String queryAsString = """
        SELECT  (( count(*) + 2 ) AS ?count) WHERE {
            SELECT  * WHERE { ?p  <http://address>  ?c } OFFSET  2 }
        """;

        SagerScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        int nbPreempt = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
            nbPreempt += 1;

        }
        assertEquals(1, sum); // 1 result where ?count = 3
        assertEquals(1, nbPreempt);
    }



}
