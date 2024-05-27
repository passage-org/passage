package fr.gdd.sage.sager.benchmarks;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.sager.pause.Save2SPARQLTest;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class WatDivTest {

    private final static Logger log = LoggerFactory.getLogger(WatDivTest.class);
    static BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

    @Disabled
    @Test
    public void watdiv_with_1s_timeout () {
        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/" + Watdiv10M.QUERIES_PATH, Watdiv10M.blacklist);

        for (Pair<String, String> nameAndQuery : queries) {
            String[] splitted = nameAndQuery.getLeft().split("/");
            String name = splitted[splitted.length-1];
            String query = nameAndQuery.getRight();
            log.debug("Executing query {}…", name);

            int nbResults = 0;
            int nbPreempt = -1;
            long start = System.currentTimeMillis();
            while (Objects.nonNull(query)) {
                log.debug(query);
                var result = Save2SPARQLTest.executeQueryWithTimeout(query, watdivBlazegraph, 60000L); // 1s timeout
                nbResults += result.getLeft();
                query = result.getRight();
                nbPreempt += 1;
            }
            long elapsed = System.currentTimeMillis() - start;

            log.info("{} {} {} {}", name, nbPreempt, nbResults, elapsed);
        }
    }

    @Disabled
    @Test
    public void watdiv_with_default_blazegraph_engine_no_preemption () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/" + Watdiv10M.QUERIES_PATH, Watdiv10M.blacklist);

        for (Pair<String, String> nameAndQuery : queries) {
            String[] splitted = nameAndQuery.getLeft().split("/");
            String name = splitted[splitted.length-1];
            String query = nameAndQuery.getRight();
            log.debug("Executing query {}…", name);

            long start = System.currentTimeMillis();
            long nbResults = watdivBlazegraph.countQuery(query);
            long elapsed = System.currentTimeMillis() - start;

            log.info("{} {} {} {}", name, 0, nbResults, elapsed);
        }
    }


    @Disabled
    @Test
    public void longest_query_10061_with_blazegraph_engine () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        String query = """
                SELECT ?v3 ?v2 ?v4 ?v1 WHERE {
                        hint:Query hint:optimizer "None" .
                        hint:Query hint:maxParallel "1".
                        hint:Query hint:pipelinedHashJoin "false".
                        hint:Query hint:chunkSize "100" .
                        
                        <http://db.uwaterloo.ca/~galuc/wsdbm/City30> <http://www.geonames.org/ontology#parentCountry> ?v1.
                        ?v4 <http://schema.org/nationality> ?v1.
                        ?v2 <http://schema.org/eligibleRegion> ?v1.
                        ?v2 <http://schema.org/eligibleQuantity> ?v3.
                }
                """;

        long start = System.currentTimeMillis();
        long nbResults = watdivBlazegraph.countQuery(query);
        long elapsed = System.currentTimeMillis() - start;
        log.info("{} {} {} {}", "query_10061", 0, nbResults, elapsed);
    }

    @Disabled
    @Test
    public void longest_query_10061_with_our_engine () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        String query = """
                SELECT * WHERE {
                        <http://db.uwaterloo.ca/~galuc/wsdbm/City30> <http://www.geonames.org/ontology#parentCountry> ?v1.
                        ?v4 <http://schema.org/nationality> ?v1.
                        ?v2 <http://schema.org/eligibleRegion> ?v1.
                        ?v2 <http://schema.org/eligibleQuantity> ?v3.
                }
                """;

        int nbResults = 0;
        long start = System.currentTimeMillis();
        while (Objects.nonNull(query)) {
            log.debug(query);
            var result = Save2SPARQLTest.executeQueryWithTimeout(query, watdivBlazegraph, 100000000L); // 1s timeout
            nbResults += result.getLeft();
            query = result.getRight();
        }
        long elapsed = System.currentTimeMillis() - start;

        log.info("{} {} {} {}", "query_10061", 0, nbResults, elapsed);
    }

}
