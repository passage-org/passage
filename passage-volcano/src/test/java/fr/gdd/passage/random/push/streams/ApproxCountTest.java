package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
@Deprecated(forRemoval = true) // TODO not actually for removal
public class ApproxCountTest {

    private final static Logger log = LoggerFactory.getLogger(ApproxCountTest.class);

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#raw")
    public void simple_triple_pattern_test (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString = "SELECT (COUNT(*) AS ?count) WHERE { ?p <http://address> ?c }";

        var results = ExecutorUtils.executeOnce(queryAsString, builder);
        log.debug(results.toString());
        assertEquals(1, results.elementSet().size()); // whp
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("count"),
                List.of("3"))); // exact result with one TP.
        bb.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#raw")
    public void count_query_nested (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString = """
            SELECT ?p ?count WHERE {
             ?p <http://address> ?c
             { SELECT ?p (COUNT(?a) AS ?count) WHERE {
                ?p <http://own> ?a
             } GROUP BY ?p }
            }
            """;

        // throws for now
        assertThrows(UnsupportedOperationException.class, () -> ExecutorUtils.executeOnce(queryAsString, builder));
        // var results = ExecutorUtils.executeOnce(queryAsString, builder);
        // log.debug(results.toString());
        // assertEquals(1, results.elementSet().size()); // whp
        // assertTrue(MultisetResultChecking.containsAllResults(results, List.of("count"),
        //        List.of("3"))); // exact result with one TP.
        bb.close();
    }

}
