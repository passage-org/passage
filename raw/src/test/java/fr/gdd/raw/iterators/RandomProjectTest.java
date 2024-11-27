package fr.gdd.raw.iterators;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.raw.RawOpExecutorUtils;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Not much to be done here, just making sure that projects are implemented.
 */
public class RandomProjectTest {

    private final static Logger log = LoggerFactory.getLogger(RandomProjectTest.class);

    @Test
    public void simple_project_on_spo () throws RepositoryException { // as per usual
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT ?s WHERE {?s ?p ?o}";
        // Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        // assertEquals(6, results.elementSet().size()); // Alice repeated 4 times

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        // spo contains other default triples… That why we need more than 100L to
        // retrieve expected spo with high probability.
        assertTrue(results.elementSet().size() >= 6); // the 6 distinct subjects + blazegraph's own
        assertEquals(1000, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "p", "o"),
                Arrays.asList("Alice", null, null),
                Arrays.asList("Bob", null, null),
                Arrays.asList("Carol", null, null),
                Arrays.asList("cat", null, null),
                Arrays.asList("dog", null, null),
                Arrays.asList("snake", null, null)));
    }

}
