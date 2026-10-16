package fr.gdd.passage.volcano.push;

import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.qlever.QLeverNative;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.opentest4j.TestAbortedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Execute a query over a QLever index with one scan per execution, so that
 * every step has to pause and hand back a continuation query. The graph is the
 * one of Figure 2 of the Passage paper (WWW'25), and the continuation queries
 * have the shape of its Example 3.10.
 *
 * <p>This is the engine side of `passage-qlever`: the backend's own tests check
 * the scan interface, this one checks that pausing and resuming across that
 * interface still yields complete and correct results.
 *
 * <p>The test needs a native library and an index that cannot be shipped, so
 * it reports as skipped rather than passed when either is missing; see
 * `passage-qlever/README.md`.
 */
class QLeverContinuationTest {

    /** The index basename, i.e. the `-i` argument of `qlever-index`. */
    static final String INDEX_PROPERTY = "qlever.test.index";

    static final String QUERY = """
            SELECT * WHERE {
              ?human <http://ex.org/type> <http://ex.org/Human> .
              ?human <http://ex.org/occupation> ?occupation
            }""";

    static final Var HUMAN = Var.alloc("human");

    static QLeverBackend backend;

    static final String CLASS_LABEL = "QLeverContinuationTest";

    /**
     * Abort with a reason that is visible in the build output. A `@BeforeAll`
     * assumption aborts the container, which surefire summarises as zero tests
     * run rather than as skipped, so without printing it a missing index looks
     * exactly like a successful run.
     */
    static TestAbortedException skip(String reason) {
        System.err.println("SKIPPING " + CLASS_LABEL + ": " + reason);
        return new TestAbortedException(reason);
    }

    @BeforeAll
    static void openTheIndex() {
        String index = System.getProperty(INDEX_PROPERTY);
        if (index == null || index.isBlank()) {
            throw skip("-D" + INDEX_PROPERTY + " names no index. NOTE that an"
                    + " empty value counts as none, which is what an unset"
                    + " shell variable gives. See passage-qlever/README.md");
        }
        if (!Files.exists(Path.of(index + ".index.pso"))) {
            throw skip("no QLever index at " + index);
        }
        try {
            QLeverNative.load();
        } catch (UnsatisfiedLinkError e) {
            throw skip("the qleverjni library was not found. " + e.getMessage());
        }
        backend = new QLeverBackend(index);
    }

    @AfterAll
    static void closeTheIndex() throws Exception {
        if (backend != null) { backend.close(); }
    }

    /**
     * `H1` has two occupations and `H3` one, so the join has three solutions.
     * Capped at one scan per execution, reaching them takes six executions,
     * i.e. five continuation queries: each pause leaves a `UNION` of what is
     * already bound and a `SELECT ... OFFSET n` over the rest.
     */
    @Test
    void oneScanPerExecutionStillYieldsEverySolution() {
        PassageExecutionContextBuilder<Long, String> builder =
                new PassageExecutionContextBuilder<Long, String>()
                        .setName("PUSH")
                        .setMaxScans(1L)
                        .setMaxParallel(1)
                        .setBackend(backend)
                        .setExecutorFactory(ec ->
                                new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec));

        Pair<Multiset<BackendBindings<?, ?>>, Integer> executed =
                ExecutorUtils.executeWithNbContinuations(QUERY, builder);

        assertEquals(3, executed.getLeft().size(),
                "the results must be complete despite having been paused");
        assertEquals(5, executed.getRight(),
                "one scan per execution means five continuations for three solutions");
    }

    /**
     * Passage's output of a single pattern, position by position, against what
     * the graph says it must be. This is the test that was missing: the backend
     * used to start its iterators *on* the first match while `SpliteratorScan`
     * advances before reading, so every scan lost its first match and repeated
     * its last, and the totals still matched.
     *
     * <p>NOTE that it asserts against the graph rather than against a raw
     * iteration of the backend. Comparing the two would pass under either
     * convention, since a helper written in the same style as the engine is
     * wrong in the same way — which is how the bug survived a test suite.
     */
    @Test
    void everyMatchOfAPatternArrivesExactlyOnce() {
        List<String> humans = new ArrayList<>();
        PassageExecutionContextBuilder<Long, String> builder =
                new PassageExecutionContextBuilder<Long, String>()
                        .setName("PUSH")
                        .setMaxParallel(1)
                        .setBackend(backend)
                        .setExecutorFactory(ec ->
                                new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec));
        PassageExecutionContext<Long, String> context = builder.build();
        Op paused = context.executor.execute(
                Algebra.compile(QueryFactory.create(
                        "SELECT * WHERE { ?human <http://ex.org/type> <http://ex.org/Human> }")),
                solution -> humans.add(solution.getBinding(HUMAN).getString()));

        assertNull(paused, "nothing was capped, so nothing should have paused");
        assertEquals(List.of("<http://ex.org/H1>", "<http://ex.org/H2>",
                             "<http://ex.org/H3>"), humans,
                "each of the three typed subjects exactly once, the first included");
    }

    /** Without a cap, the same query runs in a single execution. */
    @Test
    void withoutACapThereIsNoContinuation() {
        PassageExecutionContextBuilder<Long, String> builder =
                new PassageExecutionContextBuilder<Long, String>()
                        .setName("PUSH")
                        .setMaxParallel(1)
                        .setBackend(backend)
                        .setExecutorFactory(ec ->
                                new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec));

        Pair<Multiset<BackendBindings<?, ?>>, Integer> executed =
                ExecutorUtils.executeWithNbContinuations(QUERY, builder);

        assertEquals(3, executed.getLeft().size());
        assertEquals(0, executed.getRight(), "nothing was paused");
    }

    /**
     * An IRI absent from the index must make its pattern match nothing. The JNI
     * bridge reports an unknown value with the same sentinel it uses for an
     * unbound component, so the danger is the opposite of an empty result: the
     * pattern would match every triple in the store.
     */
    @Test
    void aPatternOnAnAbsentIriMatchesNothingRatherThanEverything() {
        List<String> solutions = new ArrayList<>();
        PassageExecutionContext<Long, String> context =
                new PassageExecutionContextBuilder<Long, String>()
                        .setName("PUSH")
                        .setMaxParallel(1)
                        .setBackend(backend)
                        .setExecutorFactory(ec ->
                                new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec))
                        .build();
        Op paused = context.executor.execute(
                Algebra.compile(QueryFactory.create(
                        "SELECT * WHERE { ?s <http://ex.org/no-such-predicate> ?o }")),
                solution -> solutions.add(solution.toString()));

        assertNull(paused, "nothing was capped, so nothing should have paused");
        assertEquals(List.of(), solutions,
                "an unknown predicate is not a wildcard over the six triples");
    }
}
