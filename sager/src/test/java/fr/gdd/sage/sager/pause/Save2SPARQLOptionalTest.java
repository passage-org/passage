package fr.gdd.sage.sager.pause;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class Save2SPARQLOptionalTest {

    static final Logger log = LoggerFactory.getLogger(Save2SPARQLOptionalTest.class);
    static final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());

    @Test
    public void tp_with_optional_tp () {
        String queryAsString = """
               SELECT * WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";


        int sum = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
        }
        assertEquals(3, sum);
    }

//    @Test
//    public void tp_with_optional_tp_reverse_order () {
//        String queryAsString = """
//               SELECT * WHERE {
//                ?person <http://own> ?animal .
//                OPTIONAL {?person <http://address> <http://nantes>}
//               }""";
//
//        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
//        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
//        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
//        assertEquals(3, nbResults); // Alice, Alice, and Alice.
//    }
//
//    @Test
//    public void bgp_of_3_tps_and_optional () {
//        String queryAsString = """
//               SELECT * WHERE {
//                 ?person <http://address> ?address .
//                 OPTIONAL {
//                   ?person <http://own> ?animal.
//                   ?animal <http://species> ?specie
//                 }
//               }""";
//
//        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
//        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
//        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
//        assertEquals(5, nbResults); // same as "<address> OPT <own>" query
//    }
//
//    @Test
//    public void bgp_of_3_tps_and_optional_of_optional () {
//        String queryAsString = """
//               SELECT * WHERE {
//                 ?person <http://address> ?address .
//                 OPTIONAL {
//                   ?person <http://own> ?animal.
//                   OPTIONAL {?animal <http://species> ?specie}
//                 }
//               }""";
//
//        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
//        ec.getContext().set(SagerConstants.BACKEND, blazegraph);
//        int nbResults = SagerOpExecutorTest.executeWithSager(queryAsString, ec);
//        assertEquals(5, nbResults); // same as "<address> OPT <own>" query
//    }

}
