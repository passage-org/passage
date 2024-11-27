package fr.gdd.raw.iterators;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.raw.RawOpExecutorUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Deprecated // TODO need to be more tested
public class RandomBindAsTest {

    private static final Logger log = LoggerFactory.getLogger(RandomBindAsTest.class);

    @Disabled("Does not seem to pass for now.")
    @Test
    public void simple_bind_on_a_triple_pattern () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {BIND (<http://Alice> AS ?s) ?s ?p ?o}";

        var results = RawOpExecutorUtils.executeWithRaw(queryAsString, backend, 1000L);
        log.debug("{}", results);
        assertEquals(4, results.elementSet().size());
        assertEquals(1000, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "p", "o"),
                List.of("Alice", "address", "nantes"),
                List.of("Alice", "own", "cat"),
                List.of("Alice", "own", "dog"),
                List.of("Alice", "own", "snake")));
    }

}
