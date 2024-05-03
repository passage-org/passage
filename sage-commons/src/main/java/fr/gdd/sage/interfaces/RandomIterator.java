package fr.gdd.sage.interfaces;

/**
 * An iterator that enables performing random walks in the query.
 */
public interface RandomIterator {

    /**
     * Position the iterator to a random in its allowed
     * range. Therefore, building a random triple only requires to
     * call `it.getId(SPOC.SUBJECT)`, `it.getId(SPOC.PREDICATE)`,
     * `it.getId(SPOC.OBJECT)` to which we add `it.getId(SPOC.GRAPH)`
     * for quads.
     *
     * @return True if it has a next, false otherwise.
     */
    boolean random();

    /**
     * @return A -- possibly estimated -- cardinality of the pattern.
     * @throws UnsupportedOperationException The scan iterator does not know
     * how to process the cardinality.
     */
    default long cardinality() throws UnsupportedOperationException { throw new UnsupportedOperationException(); }

    /**
     * @param strength How hard the estimate should be, e.g. when the estimate
     *                 is based on random walks, this could force the number of
     *                 random walks to perform.
     * @return A -- possibly estimated -- cardinality of the pattern.
     * @throws UnsupportedOperationException The scan iterator does not know
     * how to process the cardinality.
     */
    default long cardinality(long strength) throws UnsupportedOperationException { throw new UnsupportedOperationException(); }

}
