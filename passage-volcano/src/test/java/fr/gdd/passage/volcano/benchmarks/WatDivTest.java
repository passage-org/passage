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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Disabled("Need the WatDiv dataset to work.")
@Deprecated
public class WatDivTest {

    // public static final String PATH = "/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl";
    public static final String PATH = "/Users/skoazell/Desktop/Projects/datasets/watdiv10m-blaze/watdiv10M.jnl";
    public static final String PATH_TO_QUERIES = "/Users/skoazell/Desktop/Projects/sage-jena-benchmarks/queries/watdiv10m-sage-ordered/";

    private final static Logger log = LoggerFactory.getLogger(WatDivTest.class);
    public static BlazegraphBackend watdiv;

    static {
        try {
            watdiv = new BlazegraphBackend(PATH);
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    static final long TIMEOUT = 1000L * 60L; // 1 minutes
    static final int REPEAT = 3; // 3 runs for each

    private static Stream<Pair<String, String>> queries() throws IOException {
        List<Pair<String,String>> queries = new ArrayList<>();
        try (var queryFiles = Files.newDirectoryStream(Path.of(PATH_TO_QUERIES), "*.sparql")) {
            for (var q : queryFiles) {
                queries.add(new ImmutablePair<>(q.getFileName().toString(), Files.readString(q)));
            }
        }
        return queries.stream();
    }

    public static Stream<Arguments> configurations () {
        Integer[] parallels = {1, 4}; // 1 and 4 to compare with paper's measurements
        return Arrays.stream(parallels).flatMap(parallel -> {
            try {
                return queries().flatMap(q ->
                        IntStream.rangeClosed(1, REPEAT).mapToObj(ignored ->
                                Arguments.of(
                                        new PassageExecutionContextBuilder<>()
                                                .setTimeout(TIMEOUT)
                                                .setMaxParallel(parallel)
                                                .setName("PUSH")
                                                .setExecutorFactory((ec) -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec)),
                                        q.getLeft(), // name
                                        q.getRight()) // query
                        ));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void benchmark_passage_on_watdiv (PassageExecutionContextBuilder<?,?> builder, String name, String query) {
        builder.setBackend(watdiv);
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        LongAdder counter = new LongAdder();

        long start = System.currentTimeMillis();
        ExecutorUtils.execute(query, builder, ignored -> counter.increment());
        long elapsed = System.currentTimeMillis() - start;
        log.debug("{}", builder);
        log.debug("{}: Took {} ms to process {} results.", name, elapsed, counter.longValue());
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void benchmark_passage_on_watdiv_query (PassageExecutionContextBuilder<?,?> builder, String name, String query) {
        builder.setBackend(watdiv);
        query = "SELECT * WHERE { ?s ?p ?o }";
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        LongAdder counter = new LongAdder();

        long start = System.currentTimeMillis();
        ExecutorUtils.execute(query, builder, ignored -> counter.increment());
        long elapsed = System.currentTimeMillis() - start;
        log.debug("{}", builder);
        log.debug("{}: Took {} ms to process {} results.", name, elapsed, counter.longValue());
    }

//    @Disabled
//    @Test
//    public void watdiv_with_1s_timeout () throws IOException {
//        Map<String, Long> groundTruth = readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/baseline.csv", 2);
//        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/" + Watdiv10M.QUERIES_PATH, Watdiv10M.blacklist);
//
//        PassageScan.stopping = PauseUtils4Test.stopEveryThreeScans;
//
//        Set<String> skip = Set.of("query_10122.sparql", "query_10020.sparql", "query_10061.sparql", "query_10168.sparql",
//                "query_10083.sparql");
//
//        for (Pair<String, String> nameAndQuery : queries) {
//            String[] splitted = nameAndQuery.getLeft().split("/");
//            String name = splitted[splitted.length-1];
//            // if (!Objects.equals(name, "query_10088.sparql")) { continue ; }
//            if (skip.contains(name)) {continue;}
//            String query = nameAndQuery.getRight();
//            log.info("Executing query {}…", name);
//
//            int nbResults = 0;
//            int nbPreempt = -1;
//            long start = System.currentTimeMillis();
//            while (Objects.nonNull(query)) {
//                log.debug(query);
//                var result = PauseUtils4Test.executeQuery(query, watdivBlazegraph);
//                // var result = Save2SPARQLTest.executeQueryWithTimeout(query, watdivBlazegraph, 60000L); // 1s timeout
//                nbResults += result.getLeft();
//                query = result.getRight();
//                nbPreempt += 1;
//            }
//            long elapsed = System.currentTimeMillis() - start;
//            assertEquals (groundTruth.get(name), (long) nbResults);
//            log.info("{} {} {} {}", name, nbPreempt, nbResults, elapsed);
//        }
//    }

//
//
//    @Disabled
//    @Test
//    public void longest_query_10061_with_blazegraph_engine () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
//        String query = """
//                SELECT ?v3 ?v2 ?v4 ?v1 WHERE {
//                        hint:Query hint:optimizer "None" .
//                        hint:Query hint:maxParallel "1".
//                        hint:Query hint:pipelinedHashJoin "false".
//                        hint:Query hint:chunkSize "100" .
//
//                        <http://db.uwaterloo.ca/~galuc/wsdbm/City30> <http://www.geonames.org/ontology#parentCountry> ?v1.
//                        ?v4 <http://schema.org/nationality> ?v1.
//                        ?v2 <http://schema.org/eligibleRegion> ?v1.
//                        ?v2 <http://schema.org/eligibleQuantity> ?v3.
//                }
//                """;
//
//        long start = System.currentTimeMillis();
//        long nbResults = watdivBlazegraph.countQuery(query);
//        long elapsed = System.currentTimeMillis() - start;
//        log.info("{} {} {} {}", "query_10061", 0, nbResults, elapsed);
//    }
//
//    @Disabled
//    @Test
//    public void longest_query_10061_with_our_engine () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
//        String query = """
//                SELECT * WHERE {
//                        <http://db.uwaterloo.ca/~galuc/wsdbm/City30> <http://www.geonames.org/ontology#parentCountry> ?v1.
//                        ?v4 <http://schema.org/nationality> ?v1.
//                        ?v2 <http://schema.org/eligibleRegion> ?v1.
//                        ?v2 <http://schema.org/eligibleQuantity> ?v3.
//                }
//                """;
//
//        int nbResults = 0;
//        long start = System.currentTimeMillis();
//        while (Objects.nonNull(query)) {
//            log.debug(query);
//            var result = PauseUtils4Test.executeQueryWithTimeout(query, watdivBlazegraph, 100000000L); // 1s timeout
//            nbResults += result.getLeft();
//            query = result.getRight();
//        }
//        long elapsed = System.currentTimeMillis() - start;
//
//        log.info("{} {} {} {}", "query_10061", 0, nbResults, elapsed);
//    }
//
////    @Disabled
////    @Test
////    public void meow () throws IOException {
////        var woof = readGroundTruth("/Users/nedelec-b-2/Desktop/Projects/sage-jena/results/baseline.csv", 2);
////        System.out.println(woof);
////    }
//
//    public static Map<String, Long> readGroundTruth(String pathAsString, Integer column) throws IOException {
//        List<List<String>> records = new ArrayList<>();
//        try (BufferedReader br = new BufferedReader(new FileReader(pathAsString))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                String[] values = line.split(",");
//                records.add(Arrays.asList(values));
//            }
//        }
//
//        // skip header
//        records.remove(0);
//
//        return records.stream().map(l -> Map.entry(l.get(0), Long.parseLong(l.get(column)))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//    }

}
