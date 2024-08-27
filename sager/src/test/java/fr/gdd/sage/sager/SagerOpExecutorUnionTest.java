package fr.gdd.sage.sager;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.databases.inmemory.IM4Jena;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class SagerOpExecutorUnionTest {

    static final Logger log = LoggerFactory.getLogger(SagerOpExecutorUnionTest.class);
    static final JenaBackend jena = new JenaBackend(IM4Jena.triple9());

    @Test
    public void execute_a_simple_union () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?a}
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(6, nbResults); // 3 triples + 3 triples
    }

    @Test
    public void execute_a_union_inside_a_triple_pattern () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p  <http://own>  ?a .
                {?a <http://species> ?s} UNION {?a <http://species> ?s}
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(6, nbResults); // (cat + dog + snake)*2
    }

}
