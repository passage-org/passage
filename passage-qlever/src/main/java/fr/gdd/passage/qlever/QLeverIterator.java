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
    int indexInBatch = 0;
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
        indexInBatch = 0;
        sizeOfBatch = QLeverNative.fill(cursor, batch, BATCH_SIZE);
    }

    @Override
    public boolean hasNext() {
        return indexInBatch < sizeOfBatch;
    }

    @Override
    public void next() {
        indexInBatch += 1;
        if (indexInBatch >= sizeOfBatch && sizeOfBatch > 0) {
            fillBatch(offsetOfBatch + sizeOfBatch);
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
            indexInBatch = (int) (to - offsetOfBatch);
            return;
        }
        QLeverNative.skip(cursor, to);
        fillBatch(to);
    }

    /** The offset of the match that this iterator currently points at. */
    @Override
    public long current() {
        return offsetOfBatch + indexInBatch;
    }

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
