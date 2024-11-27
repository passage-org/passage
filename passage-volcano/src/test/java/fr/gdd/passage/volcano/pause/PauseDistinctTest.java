package fr.gdd.passage.volcano.pause;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.interfaces.Backend;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled("Need to be theoretically checked first.")
@Deprecated // TODO much more difficult, possibly impossible
public class PauseDistinctTest {

    static final Logger log = LoggerFactory.getLogger(PauseDistinctTest.class);

    @Test
    public void tp_distinct_where_every_value_is_distinct_anyway() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT DISTINCT * WHERE { ?p <http://address> ?a }";

        int nbResults = executeAll(queryAsString, blazegraph);
        assertEquals(3, nbResults); // Alice Bob Carol
    }

    @Test
    public void tp_with_projected_so_duplicates_must_be_removed() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT DISTINCT ?a WHERE { ?p <http://address> ?a }";
        // without any specific saving, the operator will forget about previously produced
        // ?a -> Nantes, and produce it again, hence failing to provide a correct distinct

        int nbResults = executeAll(queryAsString, blazegraph);
        assertEquals(2, nbResults); // Nantes and Paris
    }


    @Test
    public void bgp_with_projected_so_duplicates_must_be_removed() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
        SELECT DISTINCT ?address WHERE {
            ?person <http://address> ?address .
            ?person <http://own> ?animal
        }""";

        int nbResults = executeAll(queryAsString, blazegraph);
        assertEquals(1, nbResults); // Nantes only since only Alice has animals

        // Produces:
        //        SELECT DISTINCT  *  WHERE {
        //          { SELECT  ?address  WHERE {
        //              { { SELECT  *  WHERE
        //                { BIND(<http://Alice> AS ?person)
        //                  BIND(<http://nantes> AS ?address)
        //                  ?person  <http://own>  ?animal }
        //                OFFSET  1 }
        //              } UNION {
        //                { SELECT  *  WHERE { ?person  <http://address>  ?address }  OFFSET  1 } ## FILTER can be pushed down here
        //                ?person  <http://own>  ?animal
        //                } }
        //         } ## here should: ORDER BY ?address
        //         FILTER ( ?address != <http://nantes> )
        //        }
        // Question is: is it still valid with reordering?
        // A) Here, we can simplify the first part of the union since BIND are FILTERED
        // B) Does it need ORDER BY ? yes, should have an OrderBy ?address on the big one
    }

    /* ************************************************************* */

    public static int executeAll(String queryAsString, Backend<?,?,Long> backend) {
        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = PauseUtils4Test.executeQuery(queryAsString, backend);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        return sum;
    }

}
