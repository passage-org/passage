package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.algebra.Op;

import java.util.Iterator;

/**
 * Iterators that are designed to be put on hold, returning a SPARQL
 * query that will produce the missing results of the paused query.
 */
public abstract class PausableIterator<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public PausableIterator(PassageExecutionContext<ID,VALUE> context, Op op) {
        context.saver.register(op, this);
    }

    /**
     * @return The paused query corresponding to this operator. It does not
     * have any argument since the operator should be self-contained to produce
     * its paused state in the form of a SPARQL query.
     */
    public Op pause() { throw new UnsupportedOperationException("pause"); }

}
