package fr.gdd.passage.volcano.push;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.benchmarks.WatDivTest;
import fr.gdd.passage.volcano.pull.PassagePullExecutor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.LongAdder;

@Disabled ("More a benchmarking thing than a test.")
public class PushVsPullTest {

    private static final Logger log = LoggerFactory.getLogger(PushVsPullTest.class);

    @RepeatedTest(5)
    public void simpleSPOWithPush () {
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        BlazegraphBackend backend = WatDivTest.watdivBlazegraph;
        String spo = "SELECT * WHERE {?s ?p ?o}";

        PassageExecutionContextBuilder context = new PassageExecutionContextBuilder<>()
                .setMaxParallel(8)
                .setMaxScans(200_000L)
                .setBackend(backend)
                .setExecutorFactory(ec -> new PassagePushExecutor((PassageExecutionContext) ec));

        LongAdder counter = new LongAdder();
        long start = System.currentTimeMillis();
        ExecutorUtils.execute(spo, context, i -> counter.increment() );
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {}ms to count {} elements.", elapsed, counter.longValue());
    }

    @RepeatedTest(5)
    public void simpleSPOWithPull () {
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        BlazegraphBackend backend = WatDivTest.watdivBlazegraph;
        String spo = "SELECT * WHERE {?s ?p ?o}";

        PassageExecutionContextBuilder context = new PassageExecutionContextBuilder<>()
                .setBackend(backend)
                .setMaxScans(200_000L)
                .setExecutorFactory(ec -> new PassagePullExecutor((PassageExecutionContext) ec));

        LongAdder counter = new LongAdder();
        long start = System.currentTimeMillis();
        ExecutorUtils.execute(spo, context, i -> counter.increment() );
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {}ms to count {} elements.", elapsed, counter.longValue());
    }

}
