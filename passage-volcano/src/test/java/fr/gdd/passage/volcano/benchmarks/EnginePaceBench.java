package fr.gdd.passage.volcano.benchmarks;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * The Java engine on the same query, index and protocol as the C++ kernel of
 * `passage-cpp`, so that the two can be compared without the comparison being
 * about something else.
 *
 * <p>The protocol is the C++ side's, because that one is the stricter: a fresh
 * process, the index opened before the clock starts, a quota of 200,000
 * SOLUTIONS — `setMaxResults`, not `setMaxScans` — so that one execution
 * produces them all and no continuation is involved, and a sink that counts
 * without asking for a single string. `BackendBindings` resolve lazily, so
 * asking would add a vocabulary lookup per variable per solution, which is
 * what made an earlier measurement of the C++ CLI 87% stdout.
 *
 * <p>Both sides must also run the SAME QLever. The figure this replaces,
 * 1,438,848 solutions/s, was measured two days earlier on a build without the
 * `fill` buffer fix that is worth 29% to the boundary — so comparing against
 * it compared two QLevers as well as two engines.
 *
 *   mvn -pl passage-volcano test -Dtest=EnginePaceBench \
 *     -Dbench.qlever=$HOME/bench-watdiv/qlever/watdiv10m
 */
public class EnginePaceBench {

    static final String INDEX_PROPERTY = "bench.qlever";
    static final String CLASS_LABEL = "EnginePaceBench";
    static final long CAP = 200_000;
    static final String QUERY_PROPERTY = "bench.query";
    // The default is the two-pattern join. Overriding it matters because the
    // two regimes answer different questions: a single pattern is one scan and
    // 200,000 tuples crossing the boundary, so it measures the crossing; the
    // join opens 9,937 inner scans, so it measures the scan start instead.
    static final String DEFAULT_QUERY =
            "SELECT * WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?c ."
                    + " ?s ?p ?o }";
    static final String QUERY =
            System.getProperty(QUERY_PROPERTY, DEFAULT_QUERY);

    static TestAbortedException skip(String reason) {
        System.err.println("SKIPPING " + CLASS_LABEL + ": " + reason);
        return new TestAbortedException(reason);
    }

    @Test
    public void theFirstTwoHundredThousandSolutions() throws Exception {
        String index = System.getProperty(INDEX_PROPERTY);
        if (index == null || index.isBlank()) {
            throw skip("-D" + INDEX_PROPERTY + " names no index");
        }
        if (!Files.exists(Path.of(index + ".index.pso"))) {
            throw skip("no QLever index at " + index);
        }

        try (QLeverBackend backend = new QLeverBackend(index)) {
            // Opening the index reads hundreds of megabytes of metadata and has
            // nothing to do with the query, so the clock starts after it, as on
            // the C++ side.
            // Run it several times in the one JVM. The first is the figure a
            // fresh process gets, which is what the C++ side always gets; the
            // last is the plateau, once the JIT has compiled the pipeline.
            // The difference between them belongs to the JVM, not to the
            // boundary, and conflating the two is how a plateau figure came to
            // be compared against a fresh-process one.
            for (int run = 1; run <= 6; run++) {
              runOnce(backend, run);
            }
        }
    }

    static void runOnce(QLeverBackend backend, int run) {
            long[] produced = {0};
            Consumer<BackendBindings<Long, String>> counting = solution -> produced[0] += 1;

            long start = System.nanoTime();
            PassageExecutionContext<Long, String> context =
                    new PassageExecutionContextBuilder<Long, String>()
                            .setName("PUSH")
                            .setMaxResults(CAP)
                            // High enough that the scan quota never fires: the
                            // pause must come from the result count, as it does
                            // on the other side.
                            .setMaxScans(Long.MAX_VALUE)
                            .setMaxParallel(1)
                            .setBackend(backend)
                            .setExecutorFactory(ec ->
                                    new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec))
                            .build();
            org.apache.jena.sparql.algebra.Op compiled =
                    Algebra.compile(QueryFactory.create(QUERY));
            // Passage reorders a BGP by cardinality before executing it, and
            // the C++ kernel has no optimiser at all: it nests the loops in
            // the order the query writes them. So the plan each one runs has
            // to be printed, or the comparison is between two plans.
            if (run == 1) {
                System.out.println("  plan Passage executes: "
                        + context.optimizer.optimize(compiled));
            }
            context.executor.execute(compiled, counting);
            double ms = (System.nanoTime() - start) / 1e6;

            System.out.printf(
                    "  run %d: %,d solutions  %.1f ms  %,d solutions/s%n",
                    run, produced[0], ms, (long) (produced[0] / ms * 1000.0));
    }
}
