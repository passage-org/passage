package fr.gdd.sage.sager;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class SagerOpExecutorDistinctTest {

    static final Logger log = LoggerFactory.getLogger(SagerOpExecutorDistinctTest.class);
    static final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void basic_trial_to_create_distinct_without_projected_variable() {
        String query = "SELECT DISTINCT * WHERE { ?p <http://address> ?a }";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);

        int nbResults = SagerOpExecutorTest.executeWithSager(query, ec);
        assertEquals(3, nbResults); // Alice, Carol, and Bob
    }

    @Test
    public void basic_trial_to_create_distinct_from_other_implemented_operators() {
        String query = "SELECT DISTINCT ?a WHERE { ?p <http://address> ?a }";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);

        int nbResults = SagerOpExecutorTest.executeWithSager(query, ec);
        assertEquals(2, nbResults); // Nantes and Paris
    }

    @Test
    public void distinct_of_bgp() {
        String query = """
        SELECT DISTINCT ?address WHERE {
            ?person <http://address> ?address.
            ?person <http://own> ?animal }
        """;

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);

        int nbResults = SagerOpExecutorTest.executeWithSager(query, ec);
        assertEquals(1, nbResults); // Nantes only, since only Alice has animals
    }

    @Test
    public void distinct_of_bgp_rewritten() {
        String query = """
        SELECT DISTINCT ?address WHERE {
            {SELECT DISTINCT ?address ?person WHERE {
                ?person <http://address> ?address .
            }}
            {SELECT DISTINCT ?person WHERE {
                ?person <http://own> ?animal .
            }}
        }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);

        int nbResults = SagerOpExecutorTest.executeWithSager(query, ec);
        assertEquals(1, nbResults); // Nantes only, since only Alice has animals
    }

}
