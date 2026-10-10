package fr.gdd.passage.qlever;

import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.opentest4j.TestAbortedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Drive the QLever backend through Passage's own interfaces, over the six
 * triple graph of Figure 2 of the Passage paper (WWW'25):
 *
 * <pre>
 * H1 type Human .   H1 occupation O1 .
 * H2 type Human .   H1 occupation O2 .
 * H3 type Human .   H3 occupation O3 .
 * </pre>
 *
 * <p>These tests need a native library and an index that this module cannot
 * carry, so they report as <em>skipped</em> rather than passed when either is
 * missing. `README.md` says how to build both and which properties to set.
 */
class QLeverBackendTest {

    static final String TYPE = "<http://ex.org/type>";
    static final String OCCUPATION = "<http://ex.org/occupation>";

    /** The index basename, i.e. the `-i` argument of `qlever-index`. */
    static final String INDEX_PROPERTY = "qlever.test.index";

    static Backend<Long, String> backend;   // deliberately the interface
    static Long any;

    static final String CLASS_LABEL = "QLeverBackendTest";

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
                    + " shell variable gives. See README.md");
        }
        if (!Files.exists(Path.of(index + ".index.pso"))) {
            throw skip("no QLever index at " + index + ". See README.md");
        }
        try {
            QLeverNative.load();
        } catch (UnsatisfiedLinkError e) {
            throw skip("the qleverjni library was not found. " + e.getMessage());
        }
        backend = new QLeverBackend(index);
        any = backend.any();
    }

    @AfterAll
    static void closeTheIndex() throws Exception {
        if (backend != null) { backend.close(); }
    }

    /**
     * NOTE: Going through a type variable is not a detour. For a caller holding
     * a concrete {@code Backend<Long, String>}, {@code getId(String, int...)}
     * and {@code getId(VALUE, int...)} are ambiguous, and Passage itself only
     * ever calls {@code getId} from generic code; see {@code BackendCache}.
     */
    static <ID, VALUE> ID lookup(Backend<ID, VALUE> b, String value) {
        return b.getId(value);
    }

    /** The subjects of a scan, in the order the permutation yields them. */
    static List<String> subjectsOf(BackendIterator<Long, String> it) {
        List<String> subjects = new ArrayList<>();
        while (it.hasNext()) {
            subjects.add(it.getString(SPOC.SUBJECT));
            it.next();
        }
        return subjects;
    }

    @Test
    void anIriResolvesToAnIdentifierAndBack() {
        Long type = lookup(backend, TYPE);
        assertNotNull(type);
        assertEquals(TYPE, backend.getValue(type));
    }

    @Test
    void aScanYieldsEveryMatchAndKnowsHowManyThereAre() {
        BackendIterator<Long, String> it =
                backend.search(any, lookup(backend, TYPE), any);
        assertEquals(3d, it.cardinality());
        assertEquals(List.of("<http://ex.org/H1>", "<http://ex.org/H2>",
                             "<http://ex.org/H3>"), subjectsOf(it));
        assertFalse(it.hasNext(), "the scan is exhausted once every match is read");
    }

    /**
     * The property every continuation relies on: the offset one iterator
     * reports repositions a freshly created one on the same solution. This is
     * what lets an execution stop and be resumed by another query.
     */
    @Test
    void theReportedOffsetRepositionsAFreshIterator() {
        Long type = lookup(backend, TYPE);
        BackendIterator<Long, String> it = backend.search(any, type, any);
        it.next();                              // stop after the first match
        long continuation = it.current();
        assertEquals(1L, continuation);
        String expected = it.getString(SPOC.SUBJECT);

        BackendIterator<Long, String> resumed = backend.search(any, type, any);
        resumed.skip(continuation);
        assertEquals(continuation, resumed.current());
        assertEquals(expected, resumed.getString(SPOC.SUBJECT));
        assertEquals(List.of("<http://ex.org/H2>", "<http://ex.org/H3>"),
                     subjectsOf(resumed));
    }

    /**
     * `occupation` relates H1 to two objects and H3 to one, so its three
     * matches carry only two distinct subjects.
     */
    @Test
    void aDistinctScanSkipsTheDuplicateSubject() {
        Long occupation = lookup(backend, OCCUPATION);
        assertEquals(List.of("<http://ex.org/H1>", "<http://ex.org/H1>",
                             "<http://ex.org/H3>"),
                     subjectsOf(backend.search(any, occupation, any)));
        assertEquals(List.of("<http://ex.org/H1>", "<http://ex.org/H3>"),
                     subjectsOf(backend.searchDistinct(any, occupation, any,
                                                       Set.of(SPOC.SUBJECT))));
    }

    @Test
    void anOffsetBeyondTheEndLeavesTheIteratorExhausted() {
        BackendIterator<Long, String> it =
                backend.search(any, lookup(backend, TYPE), any);
        it.skip(99L);
        assertFalse(it.hasNext());
    }
}
