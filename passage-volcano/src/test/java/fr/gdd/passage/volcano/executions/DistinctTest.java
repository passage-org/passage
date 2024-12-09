package fr.gdd.passage.volcano.executions;

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

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Deprecated // Should not be included as long as
@Disabled("Should not be included as long as its not handled in continuations.")
public class DistinctTest {

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void basic_trial_to_create_distinct_without_projected_variable(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = "SELECT DISTINCT * WHERE { ?p <http://address> ?a }";

        var results = ExecutorUtils.execute(query, builder);
        assertEquals(3, results.size()); // Alice, Carol, and Bob
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "nantes"), List.of("Bob", "paris"), List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void basic_trial_to_create_distinct_from_other_implemented_operators(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = "SELECT DISTINCT ?a WHERE { ?p <http://address> ?a }";

        var results = ExecutorUtils.execute(query, builder);
        assertEquals(2, results.size()); // Nantes and Paris
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("a", "p"),
                Arrays.asList("nantes", null), Arrays.asList("paris", null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void distinct_of_bgp(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
        SELECT DISTINCT ?address WHERE {
            ?person <http://address> ?address.
            ?person <http://own> ?animal }
        """;

        var results = ExecutorUtils.execute(query, builder);
        assertEquals(1, results.size()); // Nantes only, since only Alice has animals
        assertTrue(MultisetResultChecking.containsResult(results, List.of("address"), List.of("nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void distinct_of_bgp_rewritten (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
        SELECT DISTINCT ?address WHERE {
            {SELECT DISTINCT ?address ?person WHERE {
                ?person <http://address> ?address .
            }}
            {SELECT DISTINCT ?person WHERE {
                ?person <http://own> ?animal .
            }}
        }""";

        var results = ExecutorUtils.execute(query, builder);
        assertEquals(1, results.size()); // Nantes only, since only Alice has animals
        assertTrue(MultisetResultChecking.containsResult(results, List.of("address", "person"),
                Arrays.asList("nantes", null)));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void bgp_with_projected_so_duplicates_must_be_removed (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
        SELECT DISTINCT ?address WHERE {
            ?person <http://address> ?address .
            ?person <http://own> ?animal
        }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // Nantes only since only Alice has animals

        // Produces:
        //        SELECT DISTINCT  *  WHERE {
        //          { SELECT  ?address  WHERE {
        //              { { SELECT  *  WHERE
        //                { BIND(<http://Alice> AS ?person)
        //                  BIND(<http://nantes> AS ?address)
        //                  ?person  <http://own>  ?animal }
        //                OFFSET  1 }
        //              } UNION {
        //                { SELECT  *  WHERE { ?person  <http://address>  ?address }  OFFSET  1 } ## FILTER can be pushed down here
        //                ?person  <http://own>  ?animal
        //                } }
        //         } ## here should: ORDER BY ?address
        //         FILTER ( ?address != <http://nantes> )
        //        }
        // Question is: is it still valid with reordering?
        // A) Here, we can simplify the first part of the union since BIND are FILTERED
        // B) Does it need ORDER BY ? yes, should have an OrderBy ?address on the big one

        blazegraph.close();
    }

}
