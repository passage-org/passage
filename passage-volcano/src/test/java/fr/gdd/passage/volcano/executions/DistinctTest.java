package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.querypatterns.IsDistinctableQuery;
import fr.gdd.passage.volcano.transforms.DistinctQuery2QueryOfDistincts;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Deprecated // Should not be included as long as
@Disabled("Should not be included as long as its not handled in continuations.")
public class DistinctTest {

    @ParameterizedTest
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void basic_trial_to_create_distinct_without_projected_variable(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = "SELECT DISTINCT * WHERE { ?p <http://address> ?a }";
        // handled as "SELECT * WHERE { ?p <http://address> ?a }"

        var results = ExecutorUtils.execute(query, builder);
        assertEquals(3, results.size()); // Alice, Carol, and Bob
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "nantes"), List.of("Bob", "paris"), List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
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
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void distinct_of_bgp(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
        SELECT DISTINCT ?person ?address WHERE {
            ?person <http://address> ?address.
            ?person <http://own> ?animal }
        """;

        assertTrue(new IsDistinctableQuery().visit(Algebra.compile(QueryFactory.create(query))));
        Op meow = new DistinctQuery2QueryOfDistincts().visit(Algebra.compile(QueryFactory.create(query)));

        var results = ExecutorUtils.execute(query, builder);
        assertEquals(1, results.size()); // Nantes only, since only Alice has animals
        assertTrue(MultisetResultChecking.containsResult(results, List.of("address"), List.of("nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void distinct_of_bgp_but_not_link_by_projected(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        // since it's not linked by a projected distinct variable, there are no guarantee that
        // the bgp produce unique addresses, based on the underlying present assumptions.
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
        SELECT DISTINCT ?address WHERE {
            ?person <http://address> ?address.
            ?person <http://own> ?animal }
        """;

        assertFalse(new IsDistinctableQuery().visit(Algebra.compile(QueryFactory.create(query))));

        assertThrows(UnsupportedOperationException.class, () -> ExecutorUtils.execute(query, builder));
    }

    @ParameterizedTest
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void distinct_of_bgp_rewritten (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String query = """
        SELECT ?address WHERE { # no need for DISTINCT here since it's handled below
            {SELECT DISTINCT * WHERE { # is like `SELECT * WHERE {`
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

}
