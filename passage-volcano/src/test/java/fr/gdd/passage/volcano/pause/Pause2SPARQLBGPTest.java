package fr.gdd.passage.volcano.pause;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Pause2SPARQLBGPTest {

    private static final Logger log = LoggerFactory.getLogger(Pause2SPARQLBGPTest.class);

    @Test
    public void create_a_simple_query_and_pause_at_each_result () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum);
    }

    @Test
    public void create_a_bgp_query_and_pause_at_each_result () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum);
    }

    @Test
    public void create_a_bgp_query_and_pause_at_each_result_but_different_order () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
               }""";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum);
    }

    @Test
    public void bgp_with_3_tps_that_preempt () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a .
                ?p <http://address> <http://nantes> .
                ?a <http://species> ?s
               }""";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum);
    }

    @Disabled
    @Test
    public void on_watdiv_conjunctive_query_0 () throws RepositoryException, SailException {
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

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
            var result = PauseUtils4Test.executeQuery(query0, watdivBlazegraph);
            sum += result.getLeft();
            query0 = result.getRight();
        }
        assertEquals(326, sum);
    }

    @Disabled
    @Test
    public void on_watdiv_conjunctive_query_10124 () throws RepositoryException, SailException {
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

        String query10124 = """
                SELECT ?v0 ?v1 ?v2 ?v3 ?v5 WHERE {
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
            var result = PauseUtils4Test.executeQuery(query10124, watdivBlazegraph);
            sum += result.getLeft();
            query10124 = result.getRight();
            log.debug(query10124);
        }
        assertEquals(117, sum);
    }

}
