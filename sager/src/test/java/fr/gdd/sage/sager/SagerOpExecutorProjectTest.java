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
public class SagerOpExecutorProjectTest {

    static final Logger log = LoggerFactory.getLogger(SagerOpExecutorProjectTest.class);

    @Test
    public void bgp_of_1_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT ?p WHERE {?p <http://address> ?c}";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);

        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Bob, Alice, and Carol.
    }

    @Test
    public void bgp_of_2_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT ?p WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Alice, Alice, and Alice.

        queryAsString = """
               SELECT ?a WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
        nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // dog, snake and cat.

        queryAsString = """
               SELECT ?p ?a WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
        nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // both at once, similar to *
    }

}
