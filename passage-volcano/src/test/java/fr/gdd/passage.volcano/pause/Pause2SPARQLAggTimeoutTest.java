package fr.gdd.passage.volcano.pause;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Aggregates are slightly more difficult to test since everything happens
 * inside the operator. Therefore, it should not return any result before
 * having processing 1 fully.
 */
@Disabled
public class Pause2SPARQLAggTimeoutTest {

    private static final Logger log = LoggerFactory.getLogger(Pause2SPARQLBGPTimeoutTest.class);

    @Test
    public void count_of_tp_but_stops_inside_the_operator () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT (COUNT(*) AS ?count) { ?p <http://address> ?c }";

        PassageScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        int nbPreempt = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
            nbPreempt += 1;

        }
        assertEquals(1, sum); // 1 result where ?count = 3
        assertEquals(3, nbPreempt);
    }

    @Test
    public void count_of_tp_but_stops_inside_the_operator_focusing_on_last_step () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT  (( count(*) + 2 ) AS ?count) WHERE {
            SELECT  * WHERE { ?p  <http://address>  ?c } OFFSET  2 }
        """;

        PassageScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        int nbPreempt = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
            nbPreempt += 1;

        }
        assertEquals(1, sum); // 1 result where ?count = 3
        assertEquals(1, nbPreempt);
    }


    @Disabled
    @Test
    public void simple_count_on_tp_kindof_groupby_p () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        // TODO issue, when preempting the inner tp, it saves the upper
        // TODO binding with ?p=Alice and ?c=Nantes. But the COUNT filters these
        // TODO to only retrieve ?count. Therefore, not only the result is return
        // TODO but without the binding ?p and ?c…
        // TODO so maybe, re BIND the input before SELECT ?
        // TODO    maybe, add the input variables in projected values. (Best option probably)
        String queryAsString = """
        SELECT * WHERE {
            ?p <http://address> ?c .
            {SELECT (COUNT(*) AS ?count) { ?p <http://own> ?animal }}
        }
        """;

        PassageScan.stopping = Save2SPARQLTest.stopAtEveryScan;

        int sum = 0;
        int nbPreempt = 0;
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            var result = Save2SPARQLTest.executeQuery(queryAsString, blazegraph);
            sum += result.getLeft();
            queryAsString = result.getRight();
            nbPreempt += 1;

        }
        assertEquals(1, nbPreempt);
        assertEquals(3, sum); // ?count = 3 for Alice; Bob and Carol have ?count = 0
    }

    @Test
    public void meow() throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        // TODO obtain identical result to this?
        String queryAsString = """
        SELECT * WHERE {
            ?p <http://address> ?address .
            {SELECT (COUNT(*) AS ?count) ?p { ?p <http://own> ?animal } GROUP BY ?p }
        }
        """;

//        (join
//          (bgp (triple ?p <http://address> ?address))
//          (project (?count ?p)
//              (extend ((?count ?.0))
//              (group (?p) ((?.0 (count)))
//                  (bgp (triple ?p <http://own> ?animal))))))

        Op meow = Algebra.compile(QueryFactory.create(queryAsString));

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(PassageConstants.BACKEND, blazegraph);

        var results = blazegraph.executeQuery(queryAsString);
        log.debug(results.toString());
    }


}
