package fr.gdd.sage.interfaces;


import org.apache.jena.graph.Node;

import java.io.Serializable;

/**
 * An iterator over a backend that enables pausing/resuming query
 * execution. Its internal identifiers are of type `ID`, and it can
 * resume its execution using type `SKIP`.
 */
public abstract class BackendIterator<ID, VALUE, SKIP extends Serializable> implements PreemptIterator<SKIP>, RandomIterator {

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

    /**
     * Convenience for engine based on Jena's AST, this avoids making
     * useless translations between types.
     * @param code Same as `getId`.
     * @return The value of the variable as Jena `Node`.
     */
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

}
