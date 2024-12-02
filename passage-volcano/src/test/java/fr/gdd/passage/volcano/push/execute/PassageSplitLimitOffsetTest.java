package fr.gdd.passage.volcano.push.execute;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.spliterators.PassageSplitScan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PassageSplitLimitOffsetTest {

    private final static Logger log = LoggerFactory.getLogger(PassageSplitLimitOffsetTest.class);

    public void make_sure_we_dont_stop () { PassageSplitScan.stopping = (e) -> false; }

    @ParameterizedTest
    @ValueSource(ints = {1,2,5,10})
    public void simple_limit_offset_on_single_triple_pattern (int maxParallel) throws RepositoryException {
        final BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                  ?person <http://address> ?city
               } LIMIT 1""";

        var results = OpExecutorUtils.executeWithPush(queryAsString, backend, maxParallel);
        log.debug("{}", results);
        assertEquals(1, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Alice", "nantes")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Bob", "paris")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city"), List.of("Carol", "nantes")));
    }

    @ParameterizedTest
    @ValueSource(ints = {1,2,5,10})
    public void limit_offset_on_bgp (int maxParallel) throws RepositoryException {
        final BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                  ?person <http://address> ?city .
                  ?person <http://own> ?animal
               } LIMIT 2 OFFSET 1""";

        var results = OpExecutorUtils.executeWithPush(queryAsString, backend, maxParallel);
        log.debug("{}", results);
        assertEquals(2, results.size());
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "city", "animal"), List.of("Alice", "nantes", "dog")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city", "animal"), List.of("Alice", "nantes", "snake")) ||
                MultisetResultChecking.containsResult(results, List.of("person", "city", "animal"), List.of("Alice", "nantes" ,"cat")));
    }

}
