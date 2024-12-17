package fr.gdd.passage.volcano.benchmarks;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Disabled("Need the WDBench dataset to work.")
@Deprecated
public class WDBenchTest {

    private final static Logger log = LoggerFactory.getLogger(WDBenchTest.class);

    public final static String PATH = "/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl";
    public final static String PATH_TO_QUERIES = "/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/passage-wdbench-multiple-tps/";
    public static BlazegraphBackend wdbench;

    static {
        try {
            wdbench = new BlazegraphBackend(PATH);
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    static final long TIMEOUT = 1000L * 60L; // 1 minutes
    static final int REPEAT = 3; // 3 runs for each

    private static Stream<Pair<String, String>> queries() throws IOException {
        List<Pair<String,String>> queries = new ArrayList<>();
        try (var queryFiles = Files.newDirectoryStream(Path.of(PATH_TO_QUERIES), "query_358.sparql")) {
            for (var q : queryFiles) {
                queries.add(new ImmutablePair<>(q.getFileName().toString(), Files.readString(q)));
            }
        }
        return queries.stream();
    }

    public static Stream<Arguments> configurations () throws IOException {
        return queries().flatMap(q ->
            IntStream.rangeClosed(1, REPEAT).mapToObj(ignored ->
                Arguments.of(
                new PassageExecutionContextBuilder<>()
                        .setTimeout(TIMEOUT)
                        .setMaxParallel(4) // 4 to compare with paper's measurements
                        .setName("PUSH")
                        .setExecutorFactory((ec) -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec)),
                        q.getLeft(), // name
                        q.getRight()) // query
            ));
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void benchmark_passage_on_wdbench_multiple_tps (PassageExecutionContextBuilder<?,?> builder, String name, String query) {
        builder.setBackend(wdbench);
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        LongAdder counter = new LongAdder();

        long start = System.currentTimeMillis();
        ExecutorUtils.execute(query, builder, ignored -> counter.increment());
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {} ms to process {} results.", elapsed, counter.longValue());
    }


    @ParameterizedTest
    @MethodSource("configurations")
    public void benchmark_passage_on_wdbench_with_specific_query (PassageExecutionContextBuilder<?,?> builder, String name, String query) {
        query = SPO; // replaced took 4.5 minutes to process SPO.
        builder.setBackend(wdbench);//.forceOrder();
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        LongAdder counter = new LongAdder();

        long start = System.currentTimeMillis();
        ExecutorUtils.execute(query, builder, ignored -> counter.increment());
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {} ms to process {} results.", elapsed, counter.longValue());
    }

    static String SPO = "SELECT * WHERE {?s ?p ?o}";


    static String QUERY_NO_THREADS = """
    SELECT  *
WHERE
  { { {   {   {   {   { { SELECT  *
                          WHERE
                            { BIND(<http://www.wikidata.org/entity/Q148> AS ?x2)
                              ?x1  <http://www.wikidata.org/prop/direct/P17>  ?x2
                            }
                          OFFSET  237444
                        }
                      }
                    UNION
                      { { SELECT  *
                          WHERE
                            { BIND(<http://www.wikidata.org/entity/Q36> AS ?x2)
                              ?x1  <http://www.wikidata.org/prop/direct/P17>  ?x2
                            }
                          OFFSET  79383
                        }
                      }
                  }
                UNION
                  { { SELECT  *
                      WHERE
                        { BIND(<http://www.wikidata.org/entity/Q34> AS ?x2)
                          ?x1  <http://www.wikidata.org/prop/direct/P17>  ?x2
                        }
                      OFFSET  155402
                    }
                  }
              }
            UNION
              { { SELECT  *
                  WHERE
                    { BIND(<http://www.wikidata.org/entity/Q55> AS ?x2)
                      ?x1  <http://www.wikidata.org/prop/direct/P17>  ?x2
                    }
                  OFFSET  334329
                }
              }
          }
        UNION
          {   {   {   {   {   {   { { SELECT  *
                                      WHERE
                                        { ?x2  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> }
                                      OFFSET  35
                                      LIMIT   6
                                    }
                                  }
                                UNION
                                  { { SELECT  *
                                      WHERE
                                        { ?x2  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> }
                                      OFFSET  75
                                      LIMIT   1
                                    }
                                  }
                              }
                            UNION
                              { { SELECT  *
                                  WHERE
                                    { ?x2  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> }
                                  OFFSET  23
                                  LIMIT   6
                                }
                              }
                          }
                        UNION
                          { { SELECT  *
                              WHERE
                                { ?x2  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> }
                              OFFSET  89
                              LIMIT   5
                            }
                          }
                      }
                    UNION
                      { { SELECT  *
                          WHERE
                            { ?x2  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> }
                          OFFSET  41
                          LIMIT   6
                        }
                      }
                  }
                UNION
                  { { SELECT  *
                      WHERE
                        { ?x2  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> }
                      OFFSET  33
                      LIMIT   2
                    }
                  }
              }
            UNION
              { { SELECT  *
                  WHERE
                    { ?x2  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q6256> }
                  OFFSET  47
                  LIMIT   11
                }
              }
            ?x1  <http://www.wikidata.org/prop/direct/P17>  ?x2
          }
        ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q515>
      }
      ?x1  <http://www.wikidata.org/prop/direct/P6>  ?x3
    }
    ?x3  <http://www.wikidata.org/prop/direct/P39>  <http://www.wikidata.org/entity/Q30185>
  }
""";


private static String QUERY_2 = """
        SELECT  *
        WHERE
          { { { { SELECT  *
                  WHERE
                    { BIND(<http://www.wikidata.org/entity/Q183> AS ?x2)
                      ?x1  <http://www.wikidata.org/prop/direct/P17>  ?x2
                    }
                  OFFSET  616097
                }
                ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q515>
              }
              ?x1  <http://www.wikidata.org/prop/direct/P6>  ?x3
            }
            ?x3  <http://www.wikidata.org/prop/direct/P39>  <http://www.wikidata.org/entity/Q30185>
          }
        """;

//
//    @Disabled
//    @Test
//    public void execute_and_monitor_optionals_of_wdbench () throws QueryEvaluationException, MalformedQueryException, RepositoryException, IOException {
//        Map<String, Long> groundTruth = WatDivTest.readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/wdbench_opts", 2);
//        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench_opts", List.of());
//
//        List<Pair<String, String>> filtered = new ArrayList<>();
//        for (Pair<String, String> nameAndQuery : queries) {
//            Op original = Algebra.compile(QueryFactory.create(nameAndQuery.getRight()));
//            CardinalityJoinOrdering cjo = new CardinalityJoinOrdering(wdbenchBlazegraph);
//            Op reordered = cjo.visit(original);
//            if (cjo.hasCartesianProduct()) {
//                log.debug("{} filtered out.", nameAndQuery.getLeft());
//                log.debug(nameAndQuery.getRight());
//            } else {
//                filtered.add(nameAndQuery);
//            }
//        }
//
//        log.info("Kept {} queries out of {}.", filtered.size(), queries.size());
//        queries = filtered;
//
//        int i = 0; // do not redo work
//        while (i < filtered.size()) {
//            if (!filtered.get(i).getLeft().endsWith("query_359.sparql")) {
//                filtered.remove(i);
//            } else {
//                filtered.remove(i);
//                break;
//            }
//        }
//
//        log.info("Remaining: {} queries…", filtered.size());
//
//        Set<String> blacklist = Set.of("query_483.sparql", "query_403.sparql", "query_229.sparql", "query_433.sparql",
//                "query_270.sparql", "query_185.sparql", "query_375.sparql", "query_373.sparql", "query_359.sparql");
//
//        for (Pair<String, String> nameAndQuery: queries) {
//            String[] splitted = nameAndQuery.getLeft().split("/");
//            String name = splitted[splitted.length-1];
//            if (blacklist.contains(name)) {
//                log.debug("Skipping {}…", name);
//            }
//            String query = nameAndQuery.getRight();
//            log.debug("Executing query {}…", name);
//            log.debug(query);
//
//            long start = System.currentTimeMillis();
//            long nbResults = -1;
//            boolean timedout = false;
//            try {
//                nbResults = wdbenchBlazegraph.countQuery(query, TIMEOUT);
//            } catch (TimeoutException e) {
//                timedout = true;
//                nbResults = Long.parseLong(e.getMessage());
//            }
//            long elapsed = System.currentTimeMillis() - start;
//
//            log.info("{} {} {} {} {}", name, 0, nbResults, elapsed, timedout);
//        }
//    }
//
//
//    @Disabled
//    @Test
//    public void run_wdbench_opts_based_on_baseline_csv () throws IOException {
//        Map<String, Long> groundTruth = WatDivTest.readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/wdbench_opts/baseline.csv", 2);
//        Map<String, Long> executionTimes = WatDivTest.readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/wdbench_opts/baseline.csv", 3);
//
//        List<String> sortedByTimes = executionTimes.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).toList();
//
//        String queryPath = "/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench_opts";
//
//        for (String name : sortedByTimes) {
//            log.info("Executing " + name + "…");
//            if (!name.equals("query_21.sparql")) {continue;}
//            long nbResults = 0;
//            int nbPreempt = -1;
//            long start = System.currentTimeMillis();
//
//            String query = Watdiv10M.readQueryFromFile(queryPath+"/"+name).getValue();
//            Op original = Algebra.compile(QueryFactory.create(query));
//            CardinalityJoinOrdering cjo = new CardinalityJoinOrdering(wdbenchBlazegraph);
//            Op reordered = cjo.visit(original);
//            query = OpAsQuery.asQuery(reordered).toString();
//
//            // SagerScan.stopping = Save2SPARQLTest.stopEveryFiveScans;
//            try {
//            while (Objects.nonNull(query)) {
//                log.debug("nbResults so far: " + nbResults);
//                log.debug(query);
//                var result = PauseUtils4Test.executeQuery(query, wdbenchBlazegraph, 1000L);
//                nbResults += result.getLeft();
//                query = result.getRight();
//                nbPreempt += 1;
//            }
//
//            long elapsed = System.currentTimeMillis() - start;
//            log.info("{} {} {} {}", name, nbPreempt, nbResults, elapsed);
//            assertEquals(groundTruth.get(name), nbResults);
//            } catch (NotFoundException e) {
//                log.info("Skipped.");
//            }
//        }
//    }
//
//
//
//    @Disabled
//    @Test
//    public void execute_a_particular_opt_query_with_preempt () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
//        String name = "meow";
//        String query = """
//                SELECT * WHERE { # 1598 results expected
//                  ?x1 <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q850270> .
//                  OPTIONAL { ?x1 <http://www.wikidata.org/prop/direct/P18> ?x2 . }
//                }
//                """;
//
//        long nbResults = 0;
//        int nbPreempt = -1;
//        long start = System.currentTimeMillis();
//        // nbResults = wdbenchBlazegraph.countQuery(query, TIMEOUT);
////        while (Objects.nonNull(query)) {
////            log.debug(query);
////            var result = Save2SPARQLTest.executeQueryWithTimeout(query, wdbenchBlazegraph, 2L); // 1s timeout
////            nbResults += result.getLeft();
////            query = result.getRight();
////            nbPreempt += 1;
////        }
//
////        while (Objects.nonNull(query)) {
////            log.debug(query);
////            var result = Save2SPARQLTest.executeQuery(query, wdbenchBlazegraph, 3L);
////            nbResults += result.getLeft();
////            query = result.getRight();
////            nbPreempt += 1;
////        }
//
//        PassageScan.stopping = PauseUtils4Test.stopEveryFiveScans;
//        while (Objects.nonNull(query)) {
//            log.debug(query);
//            var result = PauseUtils4Test.executeQuery(query, wdbenchBlazegraph, 10000L);
//            nbResults += result.getLeft();
//            query = result.getRight();
//            nbPreempt += 1;
//        }
//
//        long elapsed = System.currentTimeMillis() - start;
//        log.info("{} {} {} {}", name, nbPreempt, nbResults, elapsed);
//        assertEquals(1598, nbResults);
//    }
//
//    @Disabled
//    @Test
//    public void execute_preempted_query() throws QueryEvaluationException, MalformedQueryException, RepositoryException {
////        String queryAsString = """
////                SELECT * WHERE {
////                { SELECT  * WHERE {
////                    ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q850270>
////                } OFFSET  433 }
////                OPTIONAL {
////                    ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2
////                } }""";
//
//        // for whatever reason, the SECOND result {?x1-> <http://www.wikidata.org/entity/Q738663> ; } exists in
//        // the query above, and disappear in the query below… Does it come from UNION, or OPTIONAL?
//
//        String queryAsString = """
//                SELECT  * WHERE {
//                { { SELECT  * WHERE {
//                        BIND(<http://www.wikidata.org/entity/Q660850> AS ?x1)
//                        ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2
//                  } OFFSET  1 }
//                } UNION
//                { { SELECT  * WHERE {
//                        ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q850270>
//                  } OFFSET  433 }
//                  OPTIONAL {
//                        ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2
//                  }
//                } }""";
//
//        // below it saves wrong, skipping a result before it's consumed
//        queryAsString = """
//                SELECT  *
//                WHERE
//                  { { SELECT  *
//                      WHERE
//                        { ?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q850270> }
//                      OFFSET  434
//                    }
//                    OPTIONAL
//                      { ?x1  <http://www.wikidata.org/prop/direct/P18>  ?x2 }
//                  }
//                """;
//        var result = PauseUtils4Test.executeQuery(queryAsString, wdbenchBlazegraph, 10000L);
//        //var results = wdbenchBlazegraph.executeQuery(queryAsString);
//        // System.out.println(results);
//    }
}
