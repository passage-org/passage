package fr.gdd.passage.volcano.pause;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PauseUnionTest {

    private static final Logger log = LoggerFactory.getLogger(PauseUnionTest.class);

    @Test
    public void create_an_simple_union_that_does_not_come_from_preemption() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                {?p <http://own> ?a } UNION { ?p <http://address> <http://nantes> }
               }""";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(5, sum); // Alice * 3 + Alice + Carol
    }

    @Test
    public void create_an_simple_union_with_bgp_inside() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                {?p <http://own> ?a . ?a <http://species> ?s } UNION { ?p <http://address> <http://nantes> }
               }""";

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(5, sum); // Alice * 3 + Alice + Carol
    }
}
