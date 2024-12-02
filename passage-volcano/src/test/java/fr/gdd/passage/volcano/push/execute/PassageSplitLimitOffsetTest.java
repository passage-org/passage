package fr.gdd.passage.volcano.push.execute;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.spliterators.PassageSplitScan;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PassageSplitLimitOffsetTest {

    private final static Logger log = LoggerFactory.getLogger(PassageSplitLimitOffsetTest.class);

    public void make_sure_we_dont_stop () { PassageSplitScan.stopping = (e) -> false; }

    @Test
    public void simple_limit_offset() throws RepositoryException {
        final BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                  ?person <http://address> ?city
               } LIMIT 1""";

        var results = OpExecutorUtils.executeWithPush(queryAsString, backend);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Alice", "nantes")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Bob", "paris")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Carol", "nantes")));
    }

}
