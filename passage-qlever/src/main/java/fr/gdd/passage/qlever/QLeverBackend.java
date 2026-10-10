package fr.gdd.passage.qlever;

import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;

import java.util.Objects;
import java.util.Set;

/**
 * A {@link Backend} backed by a QLever index, through the JNI bridge in
 * `src/libqlever/jni/QleverJni.cpp` of the QLever repository.
 *
 * <p>All the scans of one backend share a single snapshot of the index, so the
 * offsets that {@link QLeverIterator#current()} reports stay valid across the
 * scans of a sequence of continuation queries.
 */
public class QLeverBackend implements Backend<Long, String> {

    final long backend;
    final long any;

    /**
     * @param pathToLibrary the `libqleverjni` shared library, or `null` to let
     *                      {@link QLeverNative#load()} locate it.
     * @param indexBasename the basename of the QLever index, i.e. the `-i` argument
     *                      that `qlever-index` was called with.
     */
    public QLeverBackend(String pathToLibrary, String indexBasename) {
        if (pathToLibrary != null) {
            QLeverNative.load(pathToLibrary);
        } else {
            QLeverNative.load();
        }
        this.backend = QLeverNative.openBackend(indexBasename);
        this.any = QLeverNative.any();
    }

    /**
     * Open an index, locating the shared library on its own; see
     * {@link QLeverNative#load()}. This is the constructor a
     * {@link fr.gdd.passage.commons.interfaces.BackendFactory} can use, since it
     * is handed the index and nothing else.
     *
     * @param indexBasename the basename of the QLever index, i.e. the `-i` argument
     *                      that `qlever-index` was called with.
     */
    public QLeverBackend(String indexBasename) {
        this(null, indexBasename);
    }

    @Override
    public BackendIterator<Long, String> search(Long s, Long p, Long o) {
        // NOTE: A caller signals an unbound component with `null`, not with
        // `any()`; see `PassageScan`, which passes the result of `Substitutor`
        // straight through.
        return new QLeverIterator(backend, Objects.isNull(s) ? any : s,
                Objects.isNull(p) ? any : p, Objects.isNull(o) ? any : o);
    }

    /**
     * Enumerate the distinct combinations of the components named by `codes`
     * (see {@link fr.gdd.passage.commons.generics.BackendConstants} for the SPOC
     * codes). This is served by a single scan: the permutation is chosen so that
     * those components lead, hence no sorting and no hash table.
     */
    @Override
    public BackendIterator<Long, String> searchDistinct(Long s, Long p, Long o,
                                                        Set<Integer> codes) {
        int mask = 0;
        for (Integer code : codes) {
            if (code == null || code < 0 || code > 2) {
                throw new UnsupportedOperationException(
                        "the QLever backend can only deduplicate on the subject, the "
                                + "predicate or the object, not on " + code);
            }
            mask |= 1 << code;
        }
        return new QLeverIterator(backend, Objects.isNull(s) ? any : s,
                Objects.isNull(p) ? any : p, Objects.isNull(o) ? any : o, mask);
    }

    @Override
    public BackendIterator<Long, String> search(Long s, Long p, Long o, Long c) {
        // QLever stores quads, but the cursor does not expose the graph yet, so a
        // scan constrained by a graph cannot be served faithfully.
        throw new UnsupportedOperationException(
                "the QLever backend does not support quad patterns yet");
    }

    @Override
    public Long getId(String value, int... type) {
        return QLeverNative.getId(backend, value);
    }

    @Override
    public String getValue(Long id, int... type) {
        return QLeverNative.getString(backend, id);
    }

    @Override
    public String getString(Long id, int... type) {
        return QLeverNative.getString(backend, id);
    }

    @Override
    public Long any() {
        return any;
    }

    @Override
    public void close() {
        QLeverNative.closeBackend(backend);
    }
}
