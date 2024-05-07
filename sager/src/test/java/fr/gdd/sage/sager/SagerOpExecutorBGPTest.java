package fr.gdd.sage.sager;

import fr.gdd.sage.databases.inmemory.IM4Jena;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class SagerOpExecutorBGPTest {

    private static final Logger log = LoggerFactory.getLogger(SagerOpExecutorBGPTest.class);
    private static final Dataset dataset = IM4Jena.triple9();

    @Test
    public void bgp_of_1_tp () {
        ExecutionContext ec = new ExecutionContext(dataset.asDatasetGraph());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Bob, Alice, and Carol.
    }

    @Test
    public void bgp_of_2_tp () {
        ExecutionContext ec = new ExecutionContext(dataset.asDatasetGraph());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Alice, Alice, and Alice.
    }

    @Test
    public void bgp_of_3_tps () {
        ExecutionContext ec = new ExecutionContext(dataset.asDatasetGraph());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
                ?a <http://species> ?s
               }""";
        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
        assertEquals(3, nbResults); // Alice->own->cat,dog,snake
    }

}
