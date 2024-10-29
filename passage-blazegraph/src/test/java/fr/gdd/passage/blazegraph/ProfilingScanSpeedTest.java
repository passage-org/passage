package fr.gdd.passage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.spo.ISPO;
import fr.gdd.passage.commons.iterators.BackendLazyIterator;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Disabled("Only a performance check to assess the potential of multithreading.")
public class ProfilingScanSpeedTest {

    private static final Logger log = LoggerFactory.getLogger(ProfilingScanSpeedTest.class);

    @Test
    public void test_concurrent_execution_to_profile_perf() throws InterruptedException, RepositoryException, SailException {
        Assumptions.assumeTrue(Objects.nonNull(BlazegraphBackendTest.watdiv));
        BlazegraphBackend bb = BlazegraphBackendTest.watdiv;

        int numberOfThreads = 3;
        ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);

        for (int i = 0; i < numberOfThreads; i++) {
            int finalI = i;
            service.execute(() -> {
                // BlazegraphIterator.RNG = new Random(finalI);
                final long TIMEOUT = 10000;
                long start = System.currentTimeMillis();
                // Context c = dataset.getContext().copy().set(SageConstants.limit, LIMIT);

                final var any = bb.any();
                final var p_1 = bb.getId("http://xmlns.com/foaf/age", SPOC.PREDICATE);
                BackendIterator<IV, ?, ?> i_1 = bb.search(any, p_1, any);
                BlazegraphIterator bi = (BlazegraphIterator) ((BackendLazyIterator) i_1).getWrapped();
                long sum = 0;
                while (System.currentTimeMillis() < start+TIMEOUT) {
                    ISPO r = bi.getUniformRandomSPO();
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
