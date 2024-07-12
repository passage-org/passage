package fr.gdd.sage.rawer.multithread;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.rawer.RawerOpExecutorTest;
import fr.gdd.sage.rawer.iterators.RandomAggregator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled
public class RawerMultithreadAggregateTest {

    private final static Logger log = LoggerFactory.getLogger(RawerMultithreadAggregateTest.class);
    static BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl");

    @Disabled
    @Test
    public void multithreading_should_be_faster () {
        // chao lee is not expected to be sample efficient…
        String queryAsString = "SELECT (COUNT( DISTINCT ?s ) AS ?count) WHERE { ?s ?p ?o }";
        RandomAggregator.SUBQUERY_LIMIT = 1;
        RawerOpExecutor executor = new RawerOpExecutor();
        System.currentTimeMillis();
        executor.setBackend(watdivBlazegraph).setLimit(1_000_000L).setMaxThreads(10);
        RawerOpExecutorTest.execute(queryAsString, executor); // 521,585 (+blaze default ones)
    }

    @Disabled
    @Test
    public void multithreading_should_be_faster_with_count () {
        // chao lee is not expected to be sample efficient…
        String queryAsString = "SELECT (COUNT( * ) AS ?count) WHERE { ?s ?p ?o }";
        RandomAggregator.SUBQUERY_LIMIT = 1;
        RawerOpExecutor executor = new RawerOpExecutor();
        System.currentTimeMillis();
        executor.setBackend(watdivBlazegraph).setLimit(1_000_000L).setMaxThreads(10);
        RawerOpExecutorTest.execute(queryAsString, executor); // 521,585 (+blaze default ones)
    }

    @Disabled
    @Test
    public void multithreading_should_be_faster_with_count_long () {
        BlazegraphBackend watdivBlazegraph1B = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv1b-blaze/watdiv1B.jnl");

        // chao lee is not expected to be sample efficient…
        String queryAsString = """
                SELECT (COUNT( * ) AS ?count) WHERE {
                    ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1> .
                    ?v0 <http://xmlns.com/foaf/givenName> ?v1 .
                    ?v0 <http://schema.org/nationality> ?v3 .
                    ?v2 <http://www.geonames.org/ontology#parentCountry> ?v3 .
                    ?v4 <http://schema.org/eligibleRegion> ?v3 .
                }"""; // exact is 4_169_173_508 results on watdiv10m
        //RandomAggregator.SUBQUERY_LIMIT = 1;
        RandomAggregator.SUBQUERY_LIMIT = 5*20;
        RawerOpExecutor executor = new RawerOpExecutor();
        long start = System.currentTimeMillis();
        executor.setBackend(watdivBlazegraph1B).setLimit(10_000_000L).setMaxThreads(1);
        RawerOpExecutorTest.execute(queryAsString, executor);
        long elapsed =  System.currentTimeMillis() - start;
        log.info("Took {} ms to process the estimate.", elapsed);
    }
}
