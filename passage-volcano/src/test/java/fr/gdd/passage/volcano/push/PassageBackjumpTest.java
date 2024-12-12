package fr.gdd.passage.volcano.push;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class aims at highlighting the capabilities of backjumping
 * when possible. For this, a graph is built with three layers:
 * - small cardinality with at least one variable;
 * - large cardinality with a bound variable with the first layer;
 * - very large cardinality with a bound variable with the first layer.
 * Therefore, the second layer always pass, but the fourth layer fails on
 * the variable. The computation should skip the whole second layer.
 */
@Disabled ("Not actually a test, more like a proof-of-concept.")
public class PassageBackjumpTest {

    private static final Logger log = LoggerFactory.getLogger(PassageBackjumpTest.class);

    private static final int FIRST_LAYER = 100;
    private static final int SECOND_LAYER = 10_000; // for each FIRST LAYER
    private static final int THIRD_LAYER = 10_000; // for each per FIRST LAYER

    public static List<String> getStatements () {
        List<String> statements = new ArrayList<>();
        for (int i = 0; i < FIRST_LAYER; ++i) {
            statements.add(String.format("<https://subject%s> <https://predicate1> <https://object%s> .\n", i, i));
        }
        for (int i = 0; i < FIRST_LAYER; ++i) {
            for (int j = 0; j < SECOND_LAYER; ++j) {
                statements.add(String.format("<https://subject%s> <https://predicate2> <https://object%s> .\n", i, j));
            }
        }
        for (int i = 0; i < FIRST_LAYER; ++i) {
            for (int j = 0; j < THIRD_LAYER; ++j) {
                if (i % 10 == 0)
                    statements.add(String.format("<https://subject%s> <https://predicate3> <https://object%s> .\n", i, j));
            }
        }
        return statements;
    }

    private final static String QUERY = """
            SELECT * WHERE {
                ?s1 <https://predicate1> ?o1 .
                ?s1 <https://predicate2> ?o2 . # succeed most of the time
                ?s1 <https://predicate3> <https://object0> . # fails most of the time
            }""";


    private static final BlazegraphBackend backend;

    static {
        try {
            backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.getDataset(getStatements()));
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void close() throws RepositoryException, SailException {
        backend.close(); // only useful in the boundaries of this class
    }

    @RepeatedTest(5)
    public void measuring_some_performance_of_backjumps () {
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        PassageExecutionContextBuilder<?,?> builder = new PassageExecutionContextBuilder<>()
                .setBackend(backend)
                .forceOrder()
                .backjump()
                .setExecutorFactory((ec)-> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec));

        LongAdder counter = new LongAdder();
        log.debug("Started the execution…");
        long start = System.currentTimeMillis();
        ExecutorUtils.execute(QUERY, builder, (i) -> counter.increment());
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {}ms to get {} results.", elapsed, counter.longValue());

        // When backjump is ENABLED:
        //[main] DEBUG PassageBackjumpTest - Took 548ms to get 100000 results.
        //[main] DEBUG PassageBackjumpTest - Took 246ms to get 100000 results.
        //[main] DEBUG PassageBackjumpTest - Took 237ms to get 100000 results.
        //[main] DEBUG PassageBackjumpTest - Took 236ms to get 100000 results.
        //[main] DEBUG PassageBackjumpTest - Took 229ms to get 100000 results.

        // When backjump is DISABLED:
        // [main] DEBUG PassageBackjumpTest - Took 2294ms to get 100000 results.
        // [main] DEBUG PassageBackjumpTest - Took 1966ms to get 100000 results.
        // [main] DEBUG PassageBackjumpTest - Took 1944ms to get 100000 results.
        // [main] DEBUG PassageBackjumpTest - Took 1948ms to get 100000 results.
        // [main] DEBUG PassageBackjumpTest - Took 1930ms to get 100000 results.
    }
}
