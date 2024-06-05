package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class Save2SPARQLDistinctTest {

    static final Logger log = LoggerFactory.getLogger(Save2SPARQLDistinctTest.class);
    static final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void tp_distinct_where_every_value_is_distinct_anyway() {
        String queryAsString = "SELECT DISTINCT * WHERE { ?p <http://address> ?a }";

        int nbResults = executeAll(queryAsString);
        assertEquals(3, nbResults); // Alice Bob Carol
    }

    @Test
    public void tp_with_projected_so_duplicates_must_be_removed() {
        String queryAsString = "SELECT DISTINCT ?a WHERE { ?p <http://address> ?a }";
        // without any specific saving, the operator will forget about previously produced
        // ?a -> Nantes, and produce it again, hence failing to provide a correct distinct

        int nbResults = executeAll(queryAsString);
        assertEquals(2, nbResults); // Nantes and Paris
    }


    @Test
    public void bgp_with_projected_so_duplicates_must_be_removed() {
        String queryAsString = """
        SELECT DISTINCT ?address WHERE {
            ?person <http://address> ?address .
            ?person <http://own> ?animal
        }""";

        int nbResults = executeAll(queryAsString);
        assertEquals(1, nbResults); // Nantes only since only Alice has animals
    }

    /* ************************************************************* */

    public static int executeAll(String queryAsString) {
        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        return sum;
    }

}
