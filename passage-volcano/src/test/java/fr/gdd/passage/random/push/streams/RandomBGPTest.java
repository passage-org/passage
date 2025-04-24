package fr.gdd.passage.random.push.streams;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.exceptions.InvalidContexException;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RandomBGPTest {

    private static final Logger log = LoggerFactory.getLogger(RandomBGPTest.class);

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#rawNoLimit")
    public void a_triple_pattern_that_does_not_exist (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        // even if the engine has no limit set, it should stop instantly as the triple pattern
        // immediately return 0 results.
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString = "SELECT * WHERE { ?p <http://do_not_exist> ?c }";

        assertThrows(InvalidContexException.class, () -> {
            var ignored = ExecutorUtils.executeOnce(queryAsString, builder);
        });
        bb.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#raw")
    public void simple_triple_pattern_test (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString = "SELECT * WHERE { ?p <http://address> ?c }";

        var results = ExecutorUtils.executeOnce(queryAsString, builder);
        log.debug(results.toString());
        assertEquals(3, results.elementSet().size()); // whp
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
        bb.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#raw")
    public void a_simple_bgp (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        // some people do not have animals
        String queryAsString = "SELECT * WHERE { ?p <http://address> ?c . ?p <http://own> ?a }";

        var results = ExecutorUtils.executeOnce(queryAsString, builder);
        log.debug(results.toString());
        assertEquals(3, results.elementSet().size()); // whp
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c", "a"),
                List.of("Alice", "nantes", "snake"),
                List.of("Alice", "nantes", "dog"),
                List.of("Alice", "nantes", "cat")));
        bb.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#raw")
    public void a_simple_bgp_with_multiple_roots (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString = "SELECT * WHERE { ?p <http://own> ?a . ?a <http://species> ?s}";

        var results = ExecutorUtils.executeOnce(queryAsString, builder);
        log.debug(results.toString());
        assertEquals(3, results.elementSet().size()); // whp
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "snake", "reptile"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "cat", "feline")));
        bb.close();
    }

    @Disabled
    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#raw")
    public void a_simple_bgp_with_multiple_roots_estimate_cost (PassageExecutionContextBuilder<?, ?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString = "SELECT * WHERE { ?p <http://own> ?a . ?a <http://species> ?s}";
        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        // var results = builder.build().executor.estimateCost(query);
        bb.close();
    }

}