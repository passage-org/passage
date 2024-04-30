package fr.gdd.sage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import it.unimi.dsi.fastutil.Hash;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * To help profiling a basic execution and evaluate the efficiency
 * of scans under concurrent calls. They are expected to be highly efficient since they
 * are read-only. However, as of 2023-05-06, they were not… A lock exists upon creation
 * which preclude the massive creation of scan iterators.
 */
public class ProfilingScanSpeedTest {

    private static Logger log = LoggerFactory.getLogger(ProfilingScanSpeedTest.class);

    @Disabled
    @Test
    public void test_concurrent_execution_to_profile_perf() throws InterruptedException {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

        int numberOfThreads = 3;
        ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            int finalI = i;
            service.execute(() -> {
                BlazegraphIterator.RNG = new Random(finalI);
                final long TIMEOUT = 10000;
                long start = System.currentTimeMillis();
                // Context c = dataset.getContext().copy().set(SageConstants.limit, LIMIT);

                final var any = bb.any();
                final var p_1 = bb.getId("http://xmlns.com/foaf/age", SPOC.PREDICATE);
                BackendIterator<IV, ?> i_1 = bb.search(any, p_1, any);
                BlazegraphIterator bi = (BlazegraphIterator) ((LazyIterator) i_1).getWrapped();
                long sum = 0;
                while (System.currentTimeMillis() < start+TIMEOUT) {
                    ISPO r = bi.random();
                    String materialized = r.toString(bi.store);
                    sum += 1;
                }
                // assertEquals(LIMIT, sum);
                long elapsed = System.currentTimeMillis() - start;
                log.info("{}ms for {} RWs  ({} RW/s)", elapsed, sum, (double)sum/(double)elapsed*1000.);
                latch.countDown();
            });
        }
        latch.await();
    }
}
