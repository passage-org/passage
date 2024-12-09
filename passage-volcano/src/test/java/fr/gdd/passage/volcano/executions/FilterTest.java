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

public class FilterTest {

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void simple_tp_filtered_by_one_var (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( ?address != <http://nantes> )
        }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // Bob only
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "address"),
                List.of("Bob", "paris")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void simple_tp_filtered_by_two_vars (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( (?address != <http://nantes>) || (?person != <http://Alice>) )
        }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // Bob and Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "address"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void filter_bgp_of_2_tps (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // Alice and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void simple_bgp_filtered_in_the_middle (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                FILTER (?c != <http://nantes>)
                ?p <http://own> ?a
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size()); // No one that lives outside nantes has animals
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void simple_bgp_filtered_in_the_middle_but_different_order (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
                ?p <http://address> <http://nantes>
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // No one that lives outside nantes has animals
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "snake"),
                List.of("Alice", "cat")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void filter_using_a_literal_integer (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                       ?animal <http://letters> ?number
                       FILTER (?number > 3)
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        // cat = 3 so filtered out
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal", "number"),
                List.of("snake", "5")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void filter_using_a_literal_and_a_function (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT ?animal WHERE {
                       ?person <http://own> ?animal
                       FILTER (strlen(str(?animal)) <= 8+3)
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // no snake this time
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal"),
                List.of("dog"),
                List.of("cat")));
        blazegraph.close();
    }


}
