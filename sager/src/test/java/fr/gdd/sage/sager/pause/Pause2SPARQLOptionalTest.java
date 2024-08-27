package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class Pause2SPARQLOptionalTest {

    static final Logger log = LoggerFactory.getLogger(Pause2SPARQLOptionalTest.class);
    static final BlazegraphBackend blazegraph;

    static {
        try {
            blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void tp_with_optional_tp () {
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";


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
    public void tp_with_optional_tp_reverse_order () {
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";


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
    public void bgp_of_3_tps_and_optional () {
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

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
    public void bgp_of_3_tps_and_optional_of_optional () {
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   OPTIONAL {?animal <http://species> ?specie}
                 }
               }""";

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
    public void query_with_optional_where_project_filter_too_much () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
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
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            // should return alice cat feline, and not feline all alone…
    }

}
