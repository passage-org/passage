package fr.gdd.passage.commons.interfaces;

/**
 * An iterator over a backend that enables pausing/resuming query
 * execution. Its internal identifiers are of type `ID`, and it can
 * resume its execution using type `SKIP`.
 */
public abstract class BackendIterator<ID, VALUE> implements PreemptIterator, RandomIterator {

    /**
     * @param code Typically, for basic scan operator, the code would
     * be 0 for subject, 1 for predicate etc.; while for values
     * operator, the code would depend on the variable order.
     * @return The identifier of the variable code.
     */
    public abstract ID getId(int code);

    /**
     * @param code Same as `getId`.
     * @return The value of the variable code.
     */
    public abstract VALUE getValue(int code);

    /**
     * @param code Same as `getId`.
     * @return The value of the variable code as a string.
     */
    public abstract String getString(int code);

    // /**
    //  * Convenience for engine based on Jena's AST, this avoids making
    //  * useless translations between types.
    //  * @param code Same as `getId`.
    //  * @return The value of the variable as Jena `Node`.
    //  */
    // public abstract Node getNode(int code);

    /**
     * @return true if there are other elements matching the pattern,
     * false otherwise.
     */
    public abstract boolean hasNext();

    /**
     * Iterates to the next element.
     */
    public abstract void next();
    
    /**
     * Go back to the beginning of the iterator. Enables reusing of
     * iterators.
     */
    public abstract void reset();

    public void skip(long to) {throw new UnsupportedOperationException("Cannot efficiently skip.");}

    public static <ID, VALUE> BackendIterator<ID, VALUE> empty(){
        return new BackendIterator() {

            @Override
            public Double random() {
                return 0.0;
            }

            @Override
            public double cardinality() throws UnsupportedOperationException {
                return 0.0;
            }

            @Override
            public double cardinality(long strength) throws UnsupportedOperationException {
                return 0.0;
            }

            @Override
            public long current() {
                return 0;
            }

            @Override
            public long previous() {
                return 0;
            }

            @Override
            public ID getId(int code) {
                return null;
            }

            @Override
            public VALUE getValue(int code) {
                return null;
            }

            @Override
            public String getString(int code) {
                return "";
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public void next() {

            }

            @Override
            public void reset() {

            }
        };
    }

}
