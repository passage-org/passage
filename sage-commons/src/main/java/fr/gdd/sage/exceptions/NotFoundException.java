package fr.gdd.sage.exceptions;

/**
 * The value or the identifier has not been found in the database.
 */
public class NotFoundException extends RuntimeException {

    public NotFoundException (String value) {
        super(value);
    }

}
