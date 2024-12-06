package fr.gdd.passage.volcano.exceptions;

/**
 * Thrown when the builder is not well set, i.e., some crucial variables
 * are not defined, or in incorrect state.
 */
public class InvalidContexException extends RuntimeException {
    public InvalidContexException(String message) {
        super(message);
    }
}
