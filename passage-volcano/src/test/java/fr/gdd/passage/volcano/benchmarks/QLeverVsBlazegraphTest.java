package fr.gdd.passage.volcano.benchmarks;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.qlever.QLeverNative;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.transforms.Quad2Pattern;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Compare a QLever index and a Blazegraph journal holding the same data, as
 * backends of the same engine, on the query that joins every typed subject with
 * all of its triples:
 *
 * <pre>SELECT * WHERE { ?s a ?c . ?s ?p ?o }</pre>
 *
 * <p>Two things are measured separately, because they answer different
 * questions. Whether the two agree is checked on a selective variant small
 * enough to compare answer for answer; how fast they are is measured on the
 * whole query, capped at the same number of solutions so that both do the same
 * amount of work.
 *
 * <p>NOTE: results are counted, never logged. `passage-cli` logs every
 * solution, which at this scale measures the logger rather than the engine.
 *
 * <p>Needs both databases, so it reports as skipped when either is missing:
 *
 * <pre>
 * mvn -pl passage-volcano test -Dtest=QLeverVsBlazegraphTest \
 *     -Dbench.qlever=$HOME/bench-watdiv/qlever/watdiv10m \
 *     -Dbench.blazegraph=$HOME/bench-watdiv/blaze/watdiv10M.jnl
 * </pre>
 */
class QLeverVsBlazegraphTest {

    static final String QLEVER_PROPERTY = "bench.qlever";
    static final String BLAZEGRAPH_PROPERTY = "bench.blazegraph";
    /** Solutions to produce from the whole query when timing. */
    static final String CAP_PROPERTY = "bench.solutions";
    /** Scans per execution, i.e. how often the engine has to pause and resume. */
    static final String SCANS_PROPERTY = "bench.scans";

    /** `Genre8` has the fewest instances of any WatDiv class, so this is small. */
    static final String SELECTIVE = """
            SELECT * WHERE {
              ?s a <http://db.uwaterloo.ca/~galuc/wsdbm/Genre8> .
              ?s ?p ?o
            }""";

    static final String WHOLE = "SELECT * WHERE { ?s a ?c . ?s ?p ?o }";

    /** Large enough to never bite, small enough that `cap - produced` cannot overflow. */
    static final long UNCAPPED = Long.MAX_VALUE / 2;

    static QLeverBackend qlever;
    static BlazegraphBackend blazegraph;

    static TestAbortedException skip(String reason) {
        System.err.println("SKIPPING QLeverVsBlazegraphTest: " + reason);
        return new TestAbortedException(reason);
    }

    @BeforeAll
    static void openBoth() throws Exception {
        String qleverIndex = System.getProperty(QLEVER_PROPERTY);
        String journal = System.getProperty(BLAZEGRAPH_PROPERTY);
        if (qleverIndex == null || qleverIndex.isBlank()) {
            throw skip("-D" + QLEVER_PROPERTY + " names no QLever index");
        }
        if (journal == null || journal.isBlank()) {
            throw skip("-D" + BLAZEGRAPH_PROPERTY + " names no Blazegraph journal");
        }
        if (!Files.exists(Path.of(qleverIndex + ".index.pso"))) {
            throw skip("no QLever index at " + qleverIndex);
        }
        if (!Files.exists(Path.of(journal))) {
            throw skip("no Blazegraph journal at " + journal);
        }
        qlever = new QLeverBackend(qleverIndex);
        blazegraph = new BlazegraphBackend(journal);
    }

    @AfterAll
    static void closeBoth() throws Exception {
        if (qlever != null) { qlever.close(); }
        if (blazegraph != null) { blazegraph.close(); }
    }

    /**
     * One solution rendered so that two backends can be compared despite
     * holding different kinds of identifier underneath.
     */
    static String render(BackendBindings<?, ?> solution) {
        return solution.variables().stream()
                .sorted(Comparator.comparing(Var::getVarName))
                .map(v -> v.getVarName() + "=" + solution.getBinding(v).getString())
                .collect(Collectors.joining(" "));
    }

    /** What one run produced: solutions, and how many executions it took. */
    record Outcome(long solutions, int executions) {
        /** One more than the number of continuation queries. */
        int continuations() { return executions - 1; }
    }

    /**
     * Execute `query` over `backend`, resuming from each continuation until the
     * query is done or `cap` solutions have been produced, handing every
     * solution to `sink`.
     */
    static <ID, VALUE> Outcome run(Backend<ID, VALUE> backend, String query,
                                   long scansPerExecution, long cap,
                                   Consumer<BackendBindings<?, ?>> sink) {
        long[] produced = {0};
        Consumer<BackendBindings<ID, VALUE>> counting = solution -> {
            produced[0] += 1;
            sink.accept(solution);
        };

        int executions = 0;
        String current = query;
        while (Objects.nonNull(current) && produced[0] < cap) {
            PassageExecutionContext<ID, VALUE> context =
                    new PassageExecutionContextBuilder<ID, VALUE>()
                            .setName("PUSH")
                            .setMaxScans(scansPerExecution)
                            .setMaxResults(cap - produced[0])
                            .setMaxParallel(1)
                            .setBackend(backend)
                            .setExecutorFactory(ec ->
                                    new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec))
                            .build();
            Op paused = context.executor.execute(
                    Algebra.compile(QueryFactory.create(current)), counting);
            executions += 1;
            current = Objects.nonNull(paused)
                    ? OpAsQuery.asQuery(new Quad2Pattern().visit(paused)).toString()
                    : null;
        }
        return new Outcome(produced[0], executions);
    }

    @Test
    void bothBackendsGiveTheSameAnswer() {
        List<String> fromQLever = new ArrayList<>();
        run(qlever, SELECTIVE, 1_000_000L, UNCAPPED,
                solution -> fromQLever.add(render(solution)));

        List<String> fromBlazegraph = new ArrayList<>();
        run(blazegraph, SELECTIVE, 1_000_000L, UNCAPPED,
                solution -> fromBlazegraph.add(render(solution)));

        fromQLever.sort(Comparator.naturalOrder());
        fromBlazegraph.sort(Comparator.naturalOrder());
        assertEquals(fromBlazegraph, fromQLever,
                "the two backends must answer the selective query identically");
        System.out.printf("%n== the selective query has %d solutions, identical on both%n",
                fromQLever.size());
    }

    /**
     * The comparison, with each backend warmed before it is timed. Blazegraph
     * needs it: left cold it reports around 400,000 solutions/s on the join and
     * around 800,000 once warm, so a single run of each would have flattered
     * whichever went second. QLever varies by a few percent either way.
     */
    @Test
    void theBenchmark() {
        long cap = Long.parseLong(System.getProperty(CAP_PROPERTY, "200000"));
        int repeats = Integer.parseInt(System.getProperty("bench.repeats", "5"));
        List<String[]> shapes = List.of(
                new String[]{"?s ?p ?o", "SELECT * WHERE { ?s ?p ?o }"},
                new String[]{"?s a ?c", "SELECT * WHERE { ?s a ?c }"},
                new String[]{"the join", WHOLE});

        System.out.printf("%n== first %,d solutions, median of %d after a warm-up%n",
                cap, repeats);
        System.out.printf("   %-12s %14s %14s %10s%n",
                "shape", "QLever /s", "Blazegraph /s", "ratio");
        for (String[] shape : shapes) {
            long qleverRate = medianRate(qlever, shape[1], cap, repeats);
            long blazeRate = medianRate(blazegraph, shape[1], cap, repeats);
            System.out.printf("   %-12s %,14d %,14d %9.2fx%n", shape[0],
                    qleverRate, blazeRate, qleverRate / (double) blazeRate);
        }
    }

    /** Solutions per second, median of `repeats` timed runs after one warm-up. */
    static <ID, VALUE> long medianRate(Backend<ID, VALUE> backend, String query,
                                       long cap, int repeats) {
        run(backend, query, 1_000_000L, cap, solution -> { });   // warm-up
        List<Long> rates = new ArrayList<>();
        for (int i = 0; i < repeats; ++i) {
            long start = System.nanoTime();
            Outcome outcome = run(backend, query, 1_000_000L, cap, solution -> { });
            long ms = Math.max(1, (System.nanoTime() - start) / 1_000_000);
            rates.add(outcome.solutions() * 1000 / ms);
        }
        rates.sort(Comparator.naturalOrder());
        return rates.get(rates.size() / 2);
    }

    /**
     * Measure the cost of opening a scan directly, rather than deriving it from
     * the join: bind a subject, create the scan, read one solution, close. This
     * is what a nested-loop join does once per outer solution, and it goes
     * through `Backend.search` exactly as the engine would.
     */
    @Test
    void theCostOfOpeningAScan() throws Exception {
        int howMany = Integer.parseInt(System.getProperty("bench.scans.measured", "2000"));
        System.out.printf("%n== opening %,d scans on a bound subject%n", howMany);
        System.out.printf("   %-12s %10s %14s%n", "backend", "ms", "ms per scan");
        timeScans("QLever", qlever, howMany);
        timeScans("Blazegraph", blazegraph, howMany);
    }

    static <ID, VALUE> void timeScans(String name, Backend<ID, VALUE> backend, int howMany) {
        // Subjects to bind, taken from the backend itself so that each exists.
        List<ID> subjects = new ArrayList<>();
        var typed = backend.search(backend.any(), backend.getId("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"), backend.any());
        while (typed.hasNext() && subjects.size() < howMany) {
            subjects.add(typed.getId(SPOC.SUBJECT));
            typed.next();
        }

        long start = System.nanoTime();
        long solutions = 0;
        for (ID subject : subjects) {
            var scan = backend.search(subject, backend.any(), backend.any());
            if (scan.hasNext()) { scan.getId(SPOC.OBJECT); solutions += 1; }
        }
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.printf("   %-12s %,10d %14.3f   (%,d subjects, %,d read)%n",
                name, ms, ms / (double) subjects.size(), subjects.size(), solutions);
    }

    /**
     * Settle what the fixed cost of opening a QLever scan is made of. `lazyScan`
     * builds an `AsyncTransformView` over `lazy-index-scan-num-blocks-in-flight`
     * blocks for every scan, twenty by default, which a scan that reads one
     * block pays for and cannot use. If that is the cost, lowering the parameter
     * lowers it; if the cost is flat, it is elsewhere.
     *
     * <p>The parameter is global rather than per scan, and the constraint on it
     * rejects anything below one, so one is the shallowest prefetch there is.
     */
    @Test
    void doesTheBlocksInFlightParameterExplainIt() {
        int howMany = Integer.parseInt(System.getProperty("bench.scans.measured", "2000"));
        long original = QLeverNative.getBlocksInFlight();
        System.out.printf("%n== opening %,d scans against the prefetch depth%n", howMany);
        System.out.printf("   %-10s %10s %14s%n", "in flight", "ms", "ms per scan");
        try {
            for (long blocks : List.of(original, 10L, 5L, 2L, 1L)) {
                QLeverNative.setBlocksInFlight(blocks);
                timeScans(String.valueOf(blocks), qlever, howMany);
            }
        } finally {
            QLeverNative.setBlocksInFlight(original);
        }
        System.out.printf("   (Blazegraph, for scale)%n");
        timeScans("-", blazegraph, howMany);
    }

    /**
     * The cache must not change an answer. A block is cached only when its
     * postprocessing is a no-op, which should make it invisible, and the first
     * version of it was not — it was shared across permutations, so one served
     * another's block 0. This runs the real join both ways and compares the
     * solutions themselves rather than counting them.
     */
    @Test
    void theBlockCacheChangesNoAnswer() {
        long cap = Long.parseLong(System.getProperty("bench.check.solutions", "20000"));
        long original = QLeverNative.getBlockCacheSize();
        try {
            List<String> withoutCache = new ArrayList<>();
            QLeverNative.setBlockCacheSize(0);
            run(qlever, WHOLE, 1_000_000L, cap, s -> withoutCache.add(render(s)));

            List<String> withCache = new ArrayList<>();
            QLeverNative.setBlockCacheSize(16);
            run(qlever, WHOLE, 1_000_000L, cap, s -> withCache.add(render(s)));

            assertEquals(withoutCache.size(), withCache.size());
            assertEquals(withoutCache, withCache,
                    "the block cache must not change a single solution");
            System.out.printf("%n== %,d solutions identical with and without the cache%n",
                    withCache.size());
        } finally {
            QLeverNative.setBlockCacheSize(original);
        }
    }

    /**
     * What a cache of decompressed blocks is worth on the query itself, which
     * is the question the isolated per-lookup figure does not answer: once
     * lookups are cheap, the join is bounded by reading its rows.
     */
    @Test
    void doesTheBlockCacheHelpTheQuery() {
        long cap = Long.parseLong(System.getProperty(CAP_PROPERTY, "200000"));
        long original = QLeverNative.getBlockCacheSize();
        System.out.printf("%n== %s%n== first %,d solutions, against the block cache%n",
                WHOLE, cap);
        System.out.printf("   %-12s %14s%n", "cache", "solutions/s");
        int repeats = Integer.parseInt(System.getProperty("bench.repeats", "5"));
        try {
            for (long blocks : List.of(0L, 1L, 4L, 16L)) {
                QLeverNative.setBlockCacheSize(blocks);
                System.out.printf("   %-12d %,14d%n", blocks,
                        medianRate(qlever, WHOLE, cap, repeats));
            }
        } finally {
            QLeverNative.setBlockCacheSize(original);
        }
        System.out.printf("   %-12s %,14d%n", "Blazegraph",
                medianRate(blazegraph, WHOLE, cap, repeats));
    }

    @Test
    void timeTheSameNumberOfSolutions() {
        long cap = Long.parseLong(System.getProperty(CAP_PROPERTY, "100000"));
        long scans = Long.parseLong(System.getProperty(SCANS_PROPERTY, "1000000"));

        System.out.printf("%n== %s%n== first %d solutions, %d scans per execution%n",
                WHOLE, cap, scans);
        for (var backend : List.of(new Object[]{"QLever", qlever},
                                   new Object[]{"Blazegraph", blazegraph})) {
            String name = (String) backend[0];
            @SuppressWarnings("unchecked")
            Backend<Object, Object> b = (Backend<Object, Object>) backend[1];
            long start = System.nanoTime();
            Outcome outcome = run(b, WHOLE, scans, cap, solution -> { });
            long ms = (System.nanoTime() - start) / 1_000_000;
            System.out.printf("   %-11s %,10d solutions in %,7d ms  (%,d /s, %d continuations)%n",
                    name, outcome.solutions(), ms,
                    ms == 0 ? 0 : outcome.solutions() * 1000 / ms, outcome.continuations());
        }
    }
}
