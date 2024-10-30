package fr.gdd.passage.volcano.benchmarks;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.persistent.Watdiv10M;
import fr.gdd.passage.volcano.iterators.PassageScan;
import fr.gdd.passage.volcano.pause.PauseUtils4Test;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled(value = "Need the WatDiv dataset to work.")
public class WatDivTest {

    public static final String PATH = "/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl";
    private final static Logger log = LoggerFactory.getLogger(WatDivTest.class);
    static BlazegraphBackend watdivBlazegraph;

    static {
        try {
            watdivBlazegraph = new BlazegraphBackend(PATH);
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @Disabled
    @Test
    public void watdiv_spo () throws IOException {
        String query = "SELECT * WHERE {?s ?p ?o } OFFSET 20";
        int nbResults = 0;
        int nbPreempt = -1;
        long start = System.currentTimeMillis();
        while (Objects.nonNull(query)) {
            log.debug(query);
            var result = PauseUtils4Test.executeQueryWithTimeout(query, watdivBlazegraph, 100L);
            // var result = Save2SPARQLTest.executeQueryWithTimeout(query, watdivBlazegraph, 60000L); // 1s timeout
            nbResults += result.getLeft();
            query = result.getRight();
            nbPreempt += 1;
        }
        long elapsed = System.currentTimeMillis() - start;
    }

    @Disabled
    @Test
    public void watdiv_with_1s_timeout () throws IOException {
        Map<String, Long> groundTruth = readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/baseline.csv", 2);
        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/" + Watdiv10M.QUERIES_PATH, Watdiv10M.blacklist);

        PassageScan.stopping = PauseUtils4Test.stopEveryThreeScans;

        Set<String> skip = Set.of("query_10122.sparql", "query_10020.sparql", "query_10061.sparql", "query_10168.sparql",
                "query_10083.sparql");

        for (Pair<String, String> nameAndQuery : queries) {
            String[] splitted = nameAndQuery.getLeft().split("/");
            String name = splitted[splitted.length-1];
            // if (!Objects.equals(name, "query_10088.sparql")) { continue ; }
            if (skip.contains(name)) {continue;}
            String query = nameAndQuery.getRight();
            log.info("Executing query {}…", name);

            int nbResults = 0;
            int nbPreempt = -1;
            long start = System.currentTimeMillis();
            while (Objects.nonNull(query)) {
                log.debug(query);
                var result = PauseUtils4Test.executeQuery(query, watdivBlazegraph);
                // var result = Save2SPARQLTest.executeQueryWithTimeout(query, watdivBlazegraph, 60000L); // 1s timeout
                nbResults += result.getLeft();
                query = result.getRight();
                nbPreempt += 1;
            }
            long elapsed = System.currentTimeMillis() - start;
            assertEquals (groundTruth.get(name), (long) nbResults);
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
            var result = PauseUtils4Test.executeQueryWithTimeout(query, watdivBlazegraph, 100000000L); // 1s timeout
            nbResults += result.getLeft();
            query = result.getRight();
        }
        long elapsed = System.currentTimeMillis() - start;

        log.info("{} {} {} {}", "query_10061", 0, nbResults, elapsed);
    }

//    @Disabled
//    @Test
//    public void meow () throws IOException {
//        var woof = readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/baseline.csv", 2);
//        System.out.println(woof);
//    }

    public static Map<String, Long> readGroundTruth(String pathAsString, Integer column) throws IOException {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(pathAsString))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        }

        // skip header
        records.remove(0);

        return records.stream().map(l -> Map.entry(l.get(0), Long.parseLong(l.get(column)))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
