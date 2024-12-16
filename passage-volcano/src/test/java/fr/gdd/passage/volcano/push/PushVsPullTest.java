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
    private static final Long MAX_SCAN = 200_000L;

    @RepeatedTest(5)
    public void simple_SPO_with_push_one_thread () {
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        BlazegraphBackend backend = WatDivTest.watdiv;
        String spo = "SELECT * WHERE {?s ?p ?o}";

        PassageExecutionContextBuilder<?,?> context = new PassageExecutionContextBuilder<>()
                .setMaxParallel(1)
                .setMaxScans(MAX_SCAN)
                .setBackend(backend)
                .setExecutorFactory(ec -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec));

        LongAdder counter = new LongAdder();
        long start = System.currentTimeMillis();
        ExecutorUtils.execute(spo, context, i -> counter.increment() );
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {}ms to count {} elements.", elapsed, counter.longValue());
    }

    @RepeatedTest(5)
    public void simple_SPO_with_push () {
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        BlazegraphBackend backend = WatDivTest.watdiv;
        String spo = "SELECT * WHERE {?s ?p ?o}";

        PassageExecutionContextBuilder<?,?> context = new PassageExecutionContextBuilder<>()
                .setMaxParallel(8)
                .setMaxScans(MAX_SCAN)
                .setBackend(backend)
                .setSplitScans(1_000_000L)
                .setExecutorFactory(ec -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec));

        LongAdder counter = new LongAdder();
        long start = System.currentTimeMillis();
        ExecutorUtils.execute(spo, context, i -> counter.increment() );
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {}ms to count {} elements.", elapsed, counter.longValue());
    }

    @RepeatedTest(5)
    public void simple_SPO_with_pull () {
        ExecutorUtils.log = LoggerFactory.getLogger("none");
        BlazegraphBackend backend = WatDivTest.watdiv;
        String spo = "SELECT * WHERE {?s ?p ?o}";

        PassageExecutionContextBuilder<?,?> context = new PassageExecutionContextBuilder<>()
                .setBackend(backend)
                .setMaxScans(MAX_SCAN)
                .setExecutorFactory(ec -> new PassagePullExecutor<>((PassageExecutionContext<?,?>) ec));

        LongAdder counter = new LongAdder();
        long start = System.currentTimeMillis();
        ExecutorUtils.execute(spo, context, i -> counter.increment() );
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {}ms to count {} elements.", elapsed, counter.longValue());
    }

}
