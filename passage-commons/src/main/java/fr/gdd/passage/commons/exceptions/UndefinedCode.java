package fr.gdd.passage.commons.exceptions;

/**
 * Attempted to use a code that does not exist. Only SPOC exists.
 */
public class UndefinedCode extends RuntimeException {

    public UndefinedCode (int code)  {
        super(String.valueOf(code));
    }
}
