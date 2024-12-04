package fr.gdd.passage.volcano.push.pause;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PausePushJoinTest {

    private static final Logger log = LoggerFactory.getLogger(PausePushJoinTest.class);

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void create_a_bgp_query_and_pause_at_each_result (PassageExecutionContextBuilder builder) throws RepositoryException, SailException {
        log.debug("{}", builder);
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        var results = OpExecutorUtils.executeWithPush(queryAsString, builder);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "dog"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
        blazegraph.close();
    }

//    @ParameterizedTest
//    @ValueSource(ints = {1,2,5,10})
//    public void a_triple_pattern_that_is_split (int maxParallelism) throws RepositoryException, SailException {
//        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
//        String queryAsString = "SELECT * WHERE { ?s ?p ?o }";
//
//        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
//        while (Objects.nonNull(queryAsString)) {
//            log.debug(queryAsString);
//            queryAsString = OpExecutorUtils.executeWithPushPause(queryAsString, blazegraph, results::add, maxParallelism);
//        }
//        log.debug("{}", results);
//        assertEquals(9, results.size());
//        blazegraph.close();
//    }

}
