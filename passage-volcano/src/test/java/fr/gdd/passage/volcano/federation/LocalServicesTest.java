package fr.gdd.passage.volcano.federation;

import com.bigdata.rdf.sail.BigdataSail;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.writer.NQuadsWriter;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.io.ByteArrayOutputStream;
import java.util.*;

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

    @Disabled // TODO not working for now: distinct graph does not seem distinct
    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void summary_local_calling_fake_remotes (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend v0 = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.vendor0());
        final BlazegraphBackend rs41 = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.rating_site_41());
        final Map<String, BlazegraphBackend> federation = new HashMap<>();
        federation.put("http://www.vendor0.fr", v0);
        federation.put("http://www.ratingsite41.fr", rs41);

        final BlazegraphBackend summary = new BlazegraphBackend(summarize(federation));
        builder.setBackend(summary);

        String summaryQuery = "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }";

        var results = ExecutorUtils.execute(summaryQuery, builder);
        assertEquals(2, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results,
                List.of("g"), List.of("vendor0"), List.of("ratingsite41")));


        v0.close();
        rs41.close();
    }


    /**
     * @param endpoints Every endpoint url and dataset to summarize.
     * @return A `BigdataSail` containing the summarized information of the designated endpoint.
     */
    static public BigdataSail summarize(Map<String, BlazegraphBackend> endpoints) {
        List<Quad> quads2add = new ArrayList<>();
        endpoints.forEach((uri, endpoint) -> {
            PassageExecutionContextBuilder<?, ?> builder = new PassageExecutionContextBuilder<>();
            builder.setBackend(endpoint).setExecutorFactory((ec) -> new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec));
            PassagePushExecutor<?, ?> engine = new PassagePushExecutor<>(builder.build());

            engine.execute("SELECT * WHERE {?s ?p ?o}", b -> {
                quads2add.add((new ToFedUPSummary()).toSummaryQuad(new Quad(
                        NodeFactory.createURI(uri),
                        Triple.create(b.get("s"), b.get("p"), b.get("o"))), 1));
            });
        });

        ByteArrayOutputStream out = new ByteArrayOutputStream(); // ugly
        NQuadsWriter.write(out, quads2add.iterator());
        String result = out.toString();

        return BlazegraphInMemoryDatasetsFactory.getDataset(Arrays.asList(result.split("\n"))); // ugly x2
    }
}