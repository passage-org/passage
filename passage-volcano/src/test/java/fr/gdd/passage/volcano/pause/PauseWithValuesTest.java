package fr.gdd.passage.volcano.pause;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PauseWithValuesTest {

    private final static Logger log = LoggerFactory.getLogger(PauseWithValuesTest.class);

    @Test
    public void simple_values_with_pause () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> }
                ?p <http://address> ?c
            }
        """;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(1, sum); // Alice

        // assertEquals(1, results.size()); // Alice lives in Nantes
        // assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
    }

    @Test
    public void simple_values_with_multiple_values () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> }
                ?p <http://address> ?c
            }
        """;

        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
            log.debug(queryAsString);
        }
        assertEquals(2, sum); // Alice

        // assertEquals(1, results.size()); // Alice lives in Nantes
        // assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
    }

}
