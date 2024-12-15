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

}
