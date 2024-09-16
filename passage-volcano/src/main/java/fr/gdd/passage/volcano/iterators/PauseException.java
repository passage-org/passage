package fr.gdd.passage.volcano.iterators;

import org.apache.jena.sparql.algebra.Op;

/**
 * Pause Exception that interrupt the iterator pipeline execution. We don't want any stacktrace
 * not message nor nothing as it may slow down the overall execution:
 * <a href="https://www.baeldung.com/java-exceptions-performance">...</a>
 */
public class PauseException extends RuntimeException {

    public final Op caller;

    public PauseException() {
        super("Pause", null, false, false);
        this.caller = null;
    }

    public PauseException(Op caller) {
        super("Pause", null, false, false);
        this.caller = caller;
    }

}
