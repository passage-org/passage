package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.streams.IWrappedStream;
import org.apache.jena.sparql.algebra.Op;

public interface PausableStream<ID,VALUE> extends IWrappedStream<BackendBindings<ID,VALUE>> {

    /**
     * @return The paused query corresponding to this operator. It does not
     * have any argument since the operator should be self-contained to produce
     * its paused state in the form of a SPARQL query.
     */
    Op pause();

}
