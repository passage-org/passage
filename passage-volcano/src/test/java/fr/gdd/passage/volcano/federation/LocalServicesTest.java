package fr.gdd.passage.volcano.federation;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalServicesTest {

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void no_service_available(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.registerService("http://local_but_unresponsive.com", null);
        builder.setBackend(bb);
        String query = "SELECT * WHERE { SERVICE SILENT <http://local_but_unresponsive.com> { ?s ?p ?o } }";
        var results = ExecutorUtils.execute(query, builder);
        assertEquals(0, results.size());
        bb.close();
    }

    @Disabled // not sure what to test when not silent, should query execution throw, or return no results at all ?
    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void no_service_available_but_throws(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.registerService("http://local_but_unresponsive.com", null);
        builder.setBackend(bb);
        String query = "SELECT * WHERE { SERVICE <http://local_but_unresponsive.com> { ?s ?p ?o } }";
        var results = ExecutorUtils.execute(query, builder);
        assertEquals(0, results.size());
        bb.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void simple_service_that_serves_another_dataset(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        final BlazegraphBackend other = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3()); // slightly different
        builder.setBackend(bb)
                .registerService("http://summary.com", new LocalServicePassage<>(
                        new PassageExecutionContextBuilder<>()
                                .setName("REMOTE")
                                .setBackend(other)
                                .setExecutorFactory((ec)-> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec))));

        String localQuery = "SELECT * WHERE { ?s ?p ?o }";
        var results = ExecutorUtils.execute(localQuery, builder);
        assertEquals(9, results.size()); // still 9 results

        String remoteQuery = "SELECT * WHERE { SERVICE <http://summary.com> { GRAPH ?g {?s ?p ?o} } }";
        results = ExecutorUtils.execute(remoteQuery, builder);
        assertEquals(11, results.size());
        bb.close();
        other.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void simple_service_with_input(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        final BlazegraphBackend other = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3()); // slightly different
        builder.setBackend(bb)
                .registerService("http://summary.com", new LocalServicePassage<>(
                        new PassageExecutionContextBuilder<>()
                                .setName("REMOTE")
                                .setBackend(other)
                                .setExecutorFactory((ec)-> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec))));

        String remoteQuery = """
            SELECT * WHERE {
                 ?bob <http://address> <http://paris> .
                 SERVICE <http://summary.com> { GRAPH ?g {?bob <http://address> ?paris} } }
            """;
        var results = ExecutorUtils.execute(remoteQuery, builder);
        assertEquals(2, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("g", "bob", "paris"),
                List.of("Bob", "Bob", "paris"),
                List.of("Carol", "Bob", "paris"))); // Carol is les pages jaunes
        bb.close();
        other.close();
    }

}