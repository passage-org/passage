package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.databases.inmemory.IM4Jena;
import fr.gdd.sage.jena.JenaBackend;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class Pause2SPARQLUnionTest {

    private static final Logger log = LoggerFactory.getLogger(Pause2SPARQLUnionTest.class);
    private static final JenaBackend jena = new JenaBackend(IM4Jena.triple9());
    private static final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void create_an_simple_union_that_does_not_come_from_preemption() {
        String queryAsString = """
               SELECT * WHERE {
                {?p <http://own> ?a } UNION { ?p <http://address> <http://nantes> }
               }""";

        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(5, sum); // Alice * 3 + Alice + Carol
    }

    @Test
    public void create_an_simple_union_with_bgp_inside() {
        String queryAsString = """
               SELECT * WHERE {
                {?p <http://own> ?a . ?a <http://species> ?s } UNION { ?p <http://address> <http://nantes> }
               }""";

        log.debug(queryAsString);

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(5, sum); // Alice * 3 + Alice + Carol
    }
}
