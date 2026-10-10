package fr.gdd.passage.volcano.push;

import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.qlever.QLeverNative;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
 * `passage-qlever/NATIVE.md`.
 */
class QLeverContinuationTest {

    /** The index basename, i.e. the `-i` argument of `qlever-index`. */
    static final String INDEX_PROPERTY = "qlever.test.index";

    static final String QUERY = """
            SELECT * WHERE {
              ?human <http://ex.org/type> <http://ex.org/Human> .
              ?human <http://ex.org/occupation> ?occupation
            }""";

    static QLeverBackend backend;

    @BeforeAll
    static void openTheIndex() {
        String index = System.getProperty(INDEX_PROPERTY);
        assumeTrue(index != null,
                "set -D" + INDEX_PROPERTY + "=<index basename>, see passage-qlever/NATIVE.md");
        assumeTrue(Files.exists(Path.of(index + ".index.pso")),
                () -> "no QLever index at " + index);
        boolean loaded;
        try {
            QLeverNative.load();
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            loaded = false;
        }
        assumeTrue(loaded, "the qleverjni library was not found, see passage-qlever/NATIVE.md");
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
}
