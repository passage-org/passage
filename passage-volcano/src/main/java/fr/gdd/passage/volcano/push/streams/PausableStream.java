package fr.gdd.passage.volcano.push.streams;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.Op;

import java.util.stream.Stream;

public interface PausableStream<ID,VALUE> {

    /**
     * @return The wrapped stream.
     */
    Stream<BackendBindings<ID,VALUE>> stream();

    /**
     * @return The paused query corresponding to this operator. It does not
     * have any argument since the operator should be self-contained to produce
     * its paused state in the form of a SPARQL query.
     */
    // TODO remove the default once most is implem'd
    default Op pause() { throw new UnsupportedOperationException("pause"); }

}
