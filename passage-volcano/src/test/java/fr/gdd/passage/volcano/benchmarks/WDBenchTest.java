package fr.gdd.passage.volcano.benchmarks;

import com.bigdata.concurrent.TimeoutException;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.databases.persistent.Watdiv10M;
import fr.gdd.passage.volcano.iterators.scan.PassageScan;
import fr.gdd.passage.volcano.optimizers.CardinalityJoinOrdering;
import fr.gdd.passage.volcano.pause.PauseUtils4Test;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled(value = "Need the WDBench dataset to work.")
public class WDBenchTest {

    public final static String PATH = "/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl";
    private final static Logger log = LoggerFactory.getLogger(WDBenchTest.class);
    static BlazegraphBackend wdbenchBlazegraph;

    static {
        try {
            wdbenchBlazegraph = new BlazegraphBackend(PATH);
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    static final Long TIMEOUT = 1000L * 60L * 2L; // 2 minutes

    @Disabled
    @Test
    public void execute_and_monitor_optionals_of_wdbench () throws QueryEvaluationException, MalformedQueryException, RepositoryException, IOException {
        Map<String, Long> groundTruth = WatDivTest.readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/wdbench_opts", 2);
        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench_opts", List.of());

        List<Pair<String, String>> filtered = new ArrayList<>();
        for (Pair<String, String> nameAndQuery : queries) {
            Op original = Algebra.compile(QueryFactory.create(nameAndQuery.getRight()));
            CardinalityJoinOrdering cjo = new CardinalityJoinOrdering(wdbenchBlazegraph);
            Op reordered = cjo.visit(original);
            if (cjo.hasCartesianProduct()) {
                log.debug("{} filtered out.", nameAndQuery.getLeft());
                log.debug(nameAndQuery.getRight());
            } else {
                filtered.add(nameAndQuery);
            }
        }

        log.info("Kept {} queries out of {}.", filtered.size(), queries.size());
        queries = filtered;

        int i = 0; // do not redo work
        while (i < filtered.size()) {
            if (!filtered.get(i).getLeft().endsWith("query_359.sparql")) {
                filtered.remove(i);
            } else {
                filtered.remove(i);
                break;
            }
        }

        log.info("Remaining: {} queries…", filtered.size());

        Set<String> blacklist = Set.of("query_483.sparql", "query_403.sparql", "query_229.sparql", "query_433.sparql",
                "query_270.sparql", "query_185.sparql", "query_375.sparql", "query_373.sparql", "query_359.sparql");

        for (Pair<String, String> nameAndQuery: queries) {
            String[] splitted = nameAndQuery.getLeft().split("/");
            String name = splitted[splitted.length-1];
            if (blacklist.contains(name)) {
                log.debug("Skipping {}…", name);
            }
            String query = nameAndQuery.getRight();
            log.debug("Executing query {}…", name);
            log.debug(query);

            long start = System.currentTimeMillis();
            long nbResults = -1;
            boolean timedout = false;
            try {
                nbResults = wdbenchBlazegraph.countQuery(query, TIMEOUT);
            } catch (TimeoutException e) {
                timedout = true;
                nbResults = Long.parseLong(e.getMessage());
            }
            long elapsed = System.currentTimeMillis() - start;

            log.info("{} {} {} {} {}", name, 0, nbResults, elapsed, timedout);
        }
    }


    @Disabled
    @Test
    public void run_wdbench_opts_based_on_baseline_csv () throws IOException {
        Map<String, Long> groundTruth = WatDivTest.readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/wdbench_opts/baseline.csv", 2);
        Map<String, Long> executionTimes = WatDivTest.readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/wdbench_opts/baseline.csv", 3);

        List<String> sortedByTimes = executionTimes.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).toList();

        String queryPath = "/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench_opts";

        for (String name : sortedByTimes) {
            log.info("Executing " + name + "…");
            if (!name.equals("query_21.sparql")) {continue;}
            long nbResults = 0;
            int nbPreempt = -1;
            long start = System.currentTimeMillis();

            String query = Watdiv10M.readQueryFromFile(queryPath+"/"+name).getValue();
            Op original = Algebra.compile(QueryFactory.create(query));
            CardinalityJoinOrdering cjo = new CardinalityJoinOrdering(wdbenchBlazegraph);
            Op reordered = cjo.visit(original);
            query = OpAsQuery.asQuery(reordered).toString();

            // SagerScan.stopping = Save2SPARQLTest.stopEveryFiveScans;
            try {
            while (Objects.nonNull(query)) {
                log.debug("nbResults so far: " + nbResults);
                log.debug(query);
                var result = PauseUtils4Test.executeQuery(query, wdbenchBlazegraph, 1000L);
                nbResults += result.getLeft();
                query = result.getRight();
                nbPreempt += 1;
            }

            long elapsed = System.currentTimeMillis() - start;
            log.info("{} {} {} {}", name, nbPreempt, nbResults, elapsed);
            assertEquals(groundTruth.get(name), nbResults);
            } catch (NotFoundException e) {
                log.info("Skipped.");
            }
        }
    }



    @Disabled
    @Test
    public void execute_a_particular_opt_query_with_preempt () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        String name = "meow";
        String query = """
                SELECT * WHERE { # 1598 results expected
                  ?x1 <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q850270> .
                  OPTIONAL { ?x1 <http://www.wikidata.org/prop/direct/P18> ?x2 . }
                }
                """;

        long nbResults = 0;
        int nbPreempt = -1;
        long start = System.currentTimeMillis();
        // nbResults = wdbenchBlazegraph.countQuery(query, TIMEOUT);
//        while (Objects.nonNull(query)) {
//            log.debug(query);
//            var result = Save2SPARQLTest.executeQueryWithTimeout(query, wdbenchBlazegraph, 2L); // 1s timeout
//            nbResults += result.getLeft();
//            query = result.getRight();
//            nbPreempt += 1;
//        }

//        while (Objects.nonNull(query)) {
//            log.debug(query);
//            var result = Save2SPARQLTest.executeQuery(query, wdbenchBlazegraph, 3L);
//            nbResults += result.getLeft();
//            query = result.getRight();
//            nbPreempt += 1;
//        }

        PassageScan.stopping = PauseUtils4Test.stopEveryFiveScans;
        while (Objects.nonNull(query)) {
            log.debug(query);
            var result = PauseUtils4Test.executeQuery(query, wdbenchBlazegraph, 10000L);
            nbResults += result.getLeft();
            query = result.getRight();
            nbPreempt += 1;
        }

        long elapsed = System.currentTimeMillis() - start;
        log.info("{} {} {} {}", name, nbPreempt, nbResults, elapsed);
        assertEquals(1598, nbResults);
    }

    @Disabled
    @Test
    public void execute_preempted_query() throws QueryEvaluationException, MalformedQueryException, RepositoryException {
//        String queryAsString = """
//                SELECT * WHERE {
//                { SELECT  * WHERE {
//                    ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q850270>
//                } OFFSET  433 }
//                OPTIONAL {
//                    ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2
//                } }""";

        // for whatever reason, the SECOND result {?x1-> <http://www.wikidata.org/entity/Q738663> ; } exists in
        // the query above, and disappear in the query below… Does it come from UNION, or OPTIONAL?

        String queryAsString = """
                SELECT  * WHERE {
                { { SELECT  * WHERE {
                        BIND(<http://www.wikidata.org/entity/Q660850> AS ?x1)
                        ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2
                  } OFFSET  1 }
                } UNION
                { { SELECT  * WHERE {
                        ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q850270>
                  } OFFSET  433 }
                  OPTIONAL {
                        ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2
                  }
                } }""";

        // below it saves wrong, skipping a result before it's consumed
        queryAsString = """
                SELECT  *
                WHERE
                  { { SELECT  *
                      WHERE
                        { ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q850270> }
                      OFFSET  434
                    }
                    OPTIONAL
                      { ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2 }
                  }
                """;
        var result = PauseUtils4Test.executeQuery(queryAsString, wdbenchBlazegraph, 10000L);
        //var results = wdbenchBlazegraph.executeQuery(queryAsString);
        // System.out.println(results);
    }
}
