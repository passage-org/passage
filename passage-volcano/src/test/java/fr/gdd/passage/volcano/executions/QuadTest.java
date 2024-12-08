package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuadTest {

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void empty_quad_pattern (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE { GRAPH <http://Alice> { ?p <http://not_known> ?c } }";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size()); // no known so nothing
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void a_simple_quad_pattern_with_bounded_graph (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {GRAPH <http://Alice> {?p <http://address> ?c}}";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // herself
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                Arrays.asList("Alice", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void a_simple_quad_pattern_with_unknown_graph (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {GRAPH <http://David> {?p <http://address> ?c}}";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size()); // graph does not exist, so 0.
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void a_simple_quad_pattern_with_variable_for_graph (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {GRAPH ?g {?p <http://address> ?c}}";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(5, results.size()); // 1 Alice, 1 Bob, 3 Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c", "g"),
                List.of("Alice", "nantes", "Alice"),
                List.of("Bob", "paris", "Bob"),
                List.of("Alice", "nantes", "Carol"),
                List.of("Bob", "paris", "Carol"),
                List.of("Carol", "nantes", "Carol")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void multiple_graphs_are_joined (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                    GRAPH <http://Alice> {?p <http://own> ?a}.
                    GRAPH <http://Bob> {?a <http://species> ?s}
                }""";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // 3x Alice, with different species
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "snake", "reptile")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void a_graph_with_bgp_inside (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                    GRAPH <http://Alice> {
                        ?p <http://address> ?c .
                        ?p <http://own> ?a}
                }""";

        var results = OpExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // 3x Alice, with different species
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource({"fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider",
            "fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider"})
    public void bgp_with_3_tps_that_preempt (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                GRAPH ?g1 {?p <http://own> ?a .}
                GRAPH ?g2 {?p <http://address> <http://nantes> .}
                GRAPH ?g3 {?a <http://species> ?s}
               }""";


        var results = OpExecutorUtils.execute(queryAsString, builder);
        // 3x Alice, with different species, BUT this time, some tp come from multiple locations.
        // g1 -> Alice; g2 -> Alice, and Carol; g3 -> Bob
        assertEquals(6, results.size());

        assertTrue(MultisetResultChecking.containsResultTimes(results, List.of("p", "a", "s"),
                List.of("Alice", "cat", "feline"), 2));
        assertTrue(MultisetResultChecking.containsResultTimes(results, List.of("p", "a", "s"),
                List.of("Alice", "dog", "canine"), 2));
        assertTrue(MultisetResultChecking.containsResultTimes(results, List.of("p", "a", "s"),
                List.of("Alice", "snake", "reptile"), 2));
        blazegraph.close();
    }
}
