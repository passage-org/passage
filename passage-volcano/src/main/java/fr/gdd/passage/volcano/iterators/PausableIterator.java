package fr.gdd.passage.volcano.iterators;

import org.apache.jena.sparql.algebra.Op;

/**
 * Iterators that are designed to be put on hold, returning a SPARQL
 * query that will produce the missing results of the paused query.
 */
public interface PausableIterator {

    /**
     * @return The paused query corresponding to this operator. It does not
     * have any argument since the operator should be self-contained to produce
     * its paused state in the form of a SPARQL query.
     */
    Op pause();

}
