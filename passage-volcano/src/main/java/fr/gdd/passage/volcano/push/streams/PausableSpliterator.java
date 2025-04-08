package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.Op;

import java.util.Spliterator;

public interface PausableSpliterator<ID,VALUE> extends Spliterator<BackendBindings<ID,VALUE>> {

    /**
     * @return The paused query corresponding to this operator. It does not
     * have any argument since the operator should be self-contained to produce
     * its paused state in the form of a SPARQL query.
     */
    Op pause();

    /**
     * @return An estimate of the cost of exploring the whole spliterator, i.e. itself
     * plus its children if it has.
     */
    default double estimateCost() { throw new UnsupportedOperationException("Estimate cost is not supported."); }

    /**
     * @return By default, return the estimateSize of the spliterator. However, there exist
     * a conceptual difference between the two: `estimateSize` consists in the estimating the number
     * of results left for the iterator, which is the same when considering a regular execution, but
     * different when considering a random execution.
     */
    default double estimateCardinality() { return estimateSize(); }

}
