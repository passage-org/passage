package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class PassageOpExecutorFilterTest {

    static final Logger log = LoggerFactory.getLogger(PassageOpExecutorFilterTest.class);

    @Test
    public void simple_tp_filtered_by_one_var () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( ?address != <http://nantes> )
        }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(PassageConstants.BACKEND, blazegraph);

        int nbResults = PassageOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(1, nbResults); // Bob only
    }

    @Test
    public void simple_tp_filtered_by_two_vars () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( (?address != <http://nantes>) || (?person != <http://Alice>) )
        }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(PassageConstants.BACKEND, blazegraph);

        int nbResults = PassageOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(2, nbResults); // Bob and Carol
    }

}
