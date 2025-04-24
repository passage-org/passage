package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RandomProjectTest {

    private final static Logger log = LoggerFactory.getLogger(RandomProjectTest.class);

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#raw")
    public void project_a_variable_from_spo (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString = "SELECT ?p WHERE { ?p <http://address> ?c }";

        var results = ExecutorUtils.executeOnce(queryAsString, builder);
        log.debug(results.toString());
        assertEquals(3, results.elementSet().size()); // whp
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                Arrays.asList("Alice", null),
                Arrays.asList("Bob", null),
                Arrays.asList("Carol", null)));

        bb.close();
    }
}
