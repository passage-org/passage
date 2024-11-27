package fr.gdd.raw.iterators;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.raw.RawOpExecutorUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Deprecated // TODO need to be reworked to pass
public class RandomCountDistinctTest {

    private final static Logger log = LoggerFactory.getLogger(RandomCountDistinctTest.class);

    @Disabled("Timeout does not seem to work.")
    @Test
    public void count_distinct_of_simple_triple_pattern () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT (COUNT(DISTINCT ?p) AS ?c) WHERE {?p <http://own> ?a}";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1, 1000);
        log.debug("{}", results);
        assertEquals(1, results.size()); // only one : Alice, so ?c -> 1
        // it is 100% accurate since 1 result
        assertTrue(MultisetResultChecking.containsResult(results, List.of("c"), List.of("1")));
    }
}
