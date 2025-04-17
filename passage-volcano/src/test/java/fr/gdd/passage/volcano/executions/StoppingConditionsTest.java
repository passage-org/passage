package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Should check if the stopping condition defined in the execution context
 * are actually in place.
 */
public class StoppingConditionsTest {

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushOneResultAtATime")
    public void limit_the_number_of_results_on_a_triple_pattern (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        var results = ExecutorUtils.executeWithNbContinuations(queryAsString, builder);
        assertEquals(3, results.getLeft().size()); // Bob, Alice, and Carol.
        assertEquals(2, results.getRight()); // On single triple pattern, the last is checked to be empty, so only 2, not 3
        assertTrue(MultisetResultChecking.containsAllResults(results.getLeft(), List.of("p", "c"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushOneResultAtATime")
    public void limit_the_number_of_results_on_a_bgp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        var results = ExecutorUtils.executeWithNbContinuations(queryAsString, builder);
        assertEquals(3, results.getLeft().size()); // Alice, Alice, and Alice.
        assertEquals(3, results.getRight());
        assertTrue(MultisetResultChecking.containsAllResults(results.getLeft(), List.of("p", "a"),
                List.of("Alice", "dog"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
        blazegraph.close();
    }

}
