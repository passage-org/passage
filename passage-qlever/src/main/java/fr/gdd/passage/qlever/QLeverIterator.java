package fr.gdd.passage.qlever;

import fr.gdd.passage.commons.interfaces.BackendIterator;

/**
 * A scan over a triple pattern of a QLever index.
 *
 * <p>Matches are pulled from the native side in batches, because crossing the JNI
 * boundary once per match would dominate the cost of the scan itself. The batch is
 * an implementation detail: {@link #current()} always reports the offset of the
 * match that {@link #getId} returns, which is what a continuation query needs.
 */
public class QLeverIterator extends BackendIterator<Long, String> {

    /** Matches pulled from the native cursor per JNI call. */
    static final int BATCH_SIZE = 1024;

    final long backend;
    final long subject;
    final long predicate;
    final long object;

    long cursor;
    /** The current batch, three ids (subject, predicate, object) per match. */
    final long[] batch = new long[3 * BATCH_SIZE];
    int sizeOfBatch = 0;
    /**
     * The match within the batch that this iterator sits on, or `-1` before the
     * first one. `next()` moves onto a match and `getId` then reads it, which is
     * the convention `BlazegraphIterator` and `HDTIterator` both use and the one
     * `SpliteratorScan` relies on: it calls `next()` before reading anything.
     */
    int indexInBatch = -1;
    /** True once a fill has come back empty, so that `hasNext` stops asking. */
    boolean exhausted = false;
    /** The offset of `batch[0]`; the offset of the current match adds `indexInBatch`. */
    long offsetOfBatch = 0;

    /** The components whose distinct combinations are enumerated; 0 for all triples. */
    final int distinctMask;

    public QLeverIterator(long backend, Long subject, Long predicate, Long object) {
        this(backend, subject, predicate, object, 0);
    }

    public QLeverIterator(long backend, Long subject, Long predicate, Long object,
                          int distinctMask) {
        this.backend = backend;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.distinctMask = distinctMask;
        this.cursor = QLeverNative.searchDistinct(backend, subject, predicate, object,
                                                  distinctMask);
        fillBatch(0);
    }

    /** Pull the next batch, which starts at `offset` in the enumeration. */
    void fillBatch(long offset) {
        offsetOfBatch = offset;
        indexInBatch = -1;
        sizeOfBatch = QLeverNative.fill(cursor, batch, BATCH_SIZE);
        if (sizeOfBatch == 0) {
            exhausted = true;
        }
    }

    /**
     * Whether there is a match to move onto. Pulling the next batch when the
     * current one is used up happens here rather than in `next()`, because that
     * is the only way to answer the question; it leaves the batch unconsumed, so
     * asking twice gives the same answer and costs one fill.
     */
    @Override
    public boolean hasNext() {
        if (indexInBatch + 1 < sizeOfBatch) {
            return true;
        }
        if (exhausted) {
            return false;
        }
        fillBatch(offsetOfBatch + sizeOfBatch);
        return sizeOfBatch > 0;
    }

    @Override
    public void next() {
        if (hasNext()) {
            indexInBatch += 1;
        }
    }

    @Override
    public Long getId(int code) {
        return batch[3 * indexInBatch + code];
    }

    @Override
    public String getValue(int code) {
        return QLeverNative.getString(backend, getId(code));
    }

    @Override
    public String getString(int code) {
        return getValue(code);
    }

    @Override
    public void reset() {
        skip(0);
    }

    /**
     * Position the scan at `to`. Skipped matches are not read from disk where the
     * index metadata allows it.
     */
    @Override
    public void skip(long to) {
        // Staying inside the batch that is already in memory avoids a JNI call and
        // a new scan, and is the common case when skipping by a few elements.
        if (to >= offsetOfBatch && to < offsetOfBatch + sizeOfBatch) {
            // One before, so that the next `next()` moves onto `to`.
            indexInBatch = (int) (to - offsetOfBatch) - 1;
            return;
        }
        QLeverNative.skip(cursor, to);
        exhausted = false;
        fillBatch(to);
    }

    /**
     * How many matches have been consumed, which is the offset of the next one
     * and so what a continuation query resumes from. NOTE: this is one more than
     * the offset of the match `getId` returns, as in `BlazegraphIterator`, where
     * `next()` increments the offset before fetching.
     */
    @Override
    public long current() {
        return offsetOfBatch + indexInBatch + 1;
    }

    /** The offset of the match `getId` returns. */
    @Override
    public long previous() {
        long current = current();
        return current == 0 ? 0 : current - 1;
    }

    @Override
    public double cardinality() {
        return QLeverNative.cardinality(cursor);
    }

    @Override
    public double cardinality(long strength) {
        // The estimate is computed from the index metadata alone, so it is cheap
        // whatever the requested strength.
        return QLeverNative.cardinalityEstimate(cursor);
    }

    @Override
    public Double random() {
        double card = cardinality();
        if (card <= 0) {
            return 0.;
        }
        skip((long) (Math.random() * card));
        return 1. / card;
    }

    /** Release the native cursor. Using the iterator afterwards is undefined. */
    public void close() {
        if (cursor != 0) {
            QLeverNative.closeCursor(cursor);
            cursor = 0;
        }
    }
}
