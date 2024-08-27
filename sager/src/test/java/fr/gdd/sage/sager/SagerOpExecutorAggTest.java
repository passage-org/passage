package fr.gdd.sage.sager;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class SagerOpExecutorAggTest {

    static final Logger log = LoggerFactory.getLogger(SagerOpExecutorAggTest.class);

    @Disabled
    @Test
    public void simple_count_on_a_single_triple_pattern() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT (COUNT(*) AS ?count) { ?p <http://address> ?c }";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);

        int nbResults = SagerOpExecutorTest.executeWithSager(query, ec);
        assertEquals(1, nbResults); // ?count = 3
    }

    @Disabled
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
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);

        int nbResults = SagerOpExecutorTest.executeWithSager(query, ec);
        assertEquals(3, nbResults); // ?count = 3 for Alice; Bob and Carol have ?count = 0
    }

}
