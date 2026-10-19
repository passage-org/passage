package fr.gdd.passage.qlever;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The join `?s rdf:type ?c . ?s ?p ?o` through the JNI bridge, to be compared
 * against the same join run entirely in C++ by
 * `ScanCursorTest.timeAJoinAgainstARealIndex` in the QLever fork.
 *
 * <p>Why: the published comparison against Blazegraph has QLever losing this
 * join by a factor of two. Blazegraph's backend is pure Java; this one crosses
 * a foreign function boundary. So that figure cannot say which of the two it
 * measures — the storage or the bridge. The pair of benchmarks can.
 *
 * <p>This deliberately goes through {@link QLeverNative} rather than through
 * {@link QLeverBackend}, so that what is measured is the boundary and not the
 * Java-side iterator. Same loop, same batch size and same block cache as the
 * C++ side.
 */
public class JoinThroughJniBench {

    static final String INDEX_PROPERTY = "bench.qlever";
    static final String CLASS_LABEL = "JoinThroughJniBench";
    static final int TARGET = 200_000;
    static final int BATCH_SIZE = 1024;
    static final String TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

    static long backend = 0;
    /** Written by every match and never read, so the loop cannot be elided. */
    static long sink = 0;

    static TestAbortedException skip(String reason) {
        System.err.println("SKIPPING " + CLASS_LABEL + ": " + reason);
        return new TestAbortedException(reason);
    }

    @BeforeAll
    static void openTheIndex() {
        String index = System.getProperty(INDEX_PROPERTY);
        if (index == null || index.isBlank()) {
            throw skip("-D" + INDEX_PROPERTY + " names no index");
        }
        if (!Files.exists(Path.of(index + ".index.pso"))) {
            throw skip("no QLever index at " + index);
        }
        try {
            QLeverNative.load();
        } catch (UnsatisfiedLinkError e) {
            throw skip("no libqleverjni: " + e.getMessage());
        }
        QLeverNative.setBlockCacheSize(16);
        backend = QLeverNative.openBackend(index);
    }

    @AfterAll
    static void closeTheIndex() {
        if (backend != 0) { QLeverNative.closeBackend(backend); }
    }

    @Test
    public void theJoinThroughTheBoundary() {
        long any = QLeverNative.any();
        long type = QLeverNative.getId(backend, TYPE);
        long[] batch = new long[3 * BATCH_SIZE];

        // One untimed pass, so that the JIT has compiled the loop and the
        // index's working set is resident. The C++ side needs no equivalent,
        // which is itself part of what the comparison shows.
        join(any, type, batch, 20_000);

        long start = System.nanoTime();
        long solutions = join(any, type, batch, TARGET);
        double ms = (System.nanoTime() - start) / 1e6;

        System.out.printf(
                "%n  join ?s rdf:type ?c . ?s ?p ?o through JNI, first %d solutions,"
                        + " block cache 16%n    bulk, fill(%d)  %.1f ms  %d /s%n"
                        + "    (sink %d)%n%n",
                solutions, BATCH_SIZE, ms, (long) (solutions / ms * 1000.0), sink);
    }

    /** The nested loop: outer on the type predicate, inner bound on its subject. */
    static long join(long any, long type, long[] batch, long target) {
        long solutions = 0;
        long outer = QLeverNative.search(backend, any, type, any);
        try {
            while (QLeverNative.hasNext(outer) && solutions < target) {
                long subject = QLeverNative.getComponent(outer, 0);
                long inner = QLeverNative.search(backend, subject, any, any);
                try {
                    int written;
                    while ((written = QLeverNative.fill(inner, batch, BATCH_SIZE)) > 0) {
                        // Touch the object of every match, so that the scan is
                        // not optimised into counting, exactly as the C++ side
                        // does. `sink` is never read for anything else.
                        for (int i = 0; i < written; i++) {
                            sink += batch[3 * i + 2];
                        }
                        solutions += written;
                        if (solutions >= target) { break; }
                    }
                } finally {
                    QLeverNative.closeCursor(inner);
                }
                QLeverNative.next(outer);
            }
        } finally {
            QLeverNative.closeCursor(outer);
        }
        return solutions;
    }
}
