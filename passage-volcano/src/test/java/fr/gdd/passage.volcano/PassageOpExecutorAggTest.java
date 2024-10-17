package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import fr.gdd.passage.volcano.pause.Save2SPARQLTest;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PassageOpExecutorAggTest {

    static final Logger log = LoggerFactory.getLogger(PassageOpExecutorAggTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void simple_count_on_a_single_triple_pattern() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT (COUNT(*) AS ?count) { ?p <http://address> ?c }";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(BackendConstants.BACKEND, blazegraph);

        var results = PassageOpExecutorTest.executeWithPassage(query, ec);
        assertEquals(1, results.size()); // ?count = 3
    }

    @Test
    public void simple_count_on_a_single_triple_pattern_driven_by_another_one() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
        SELECT * WHERE {
            ?p <http://address> ?c .
            {SELECT (COUNT(*) AS ?count) { ?p <http://own> ?animal }}
        }
        """;

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(BackendConstants.BACKEND, blazegraph);

        var results = PassageOpExecutorTest.executeWithPassage(query, ec);
        assertEquals(3, results.size()); // ?count = 3 for Alice; Bob and Carol have ?count = 0
    }

}
