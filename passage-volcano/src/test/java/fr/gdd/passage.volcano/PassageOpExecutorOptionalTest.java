package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PassageOpExecutorOptionalTest {

    static final Logger log = LoggerFactory.getLogger(PassageOpExecutorOptionalTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void tp_with_optional_tp() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(BackendConstants.BACKEND, blazegraph);
        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, ec);
        assertEquals(5, results.size()); // Alice, Alice, and Alice, and Bob, and Carol
    }

    @Test
    public void tp_with_optional_tp_reverse_order() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(BackendConstants.BACKEND, blazegraph);
        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, ec);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
    }

    @Test
    public void bgp_of_3_tps_and_optional() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(BackendConstants.BACKEND, blazegraph);
        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, ec);
        assertEquals(5, results.size()); // same as "<address> OPT <own>" query
    }

    @Test
    public void bgp_of_3_tps_and_optional_of_optional () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   OPTIONAL {?animal <http://species> ?specie}
                 }
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(BackendConstants.BACKEND, blazegraph);
        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, ec);
        assertEquals(5, results.size()); // same as "<address> OPT <own>" query
    }

}
