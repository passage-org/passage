package fr.gdd.sage.sager;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.databases.inmemory.IM4Jena;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SagerOpExecutorBindAsTest {

    static final Logger log = LoggerFactory.getLogger(SagerOpExecutorBindAsTest.class);
    static final JenaBackend jena = new JenaBackend(IM4Jena.triple9());
    static final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());


    @Test
    public void create_a_bind_and_execute () {
        String queryAsString = """
               SELECT * WHERE {
                BIND (<http://Alice> AS ?p)
                ?p  <http://own>  ?a .
               }""";

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Alice, Alice, and Alice.
    }
}
