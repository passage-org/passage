package fr.gdd.passage.commons.interfaces;

import fr.gdd.passage.commons.exceptions.UndefinedCode;

import java.util.HashSet;
import java.util.Set;

/**
 * Code used by iterators for basic term types.
 */
public final class SPOC {
    public static final int SUBJECT   = 0;
    public static final int PREDICATE = 1;
    public static final int OBJECT    = 2;
    public static final int CONTEXT   = 3;
    public static final int GRAPH     = 3; // alias

    // useful to iterator over
    public static int[] spoc = new int []{SPOC.SUBJECT, SPOC.PREDICATE, SPOC.OBJECT, SPOC.GRAPH};

    public static String getChar(int code) {
        return switch (code) {
            case SUBJECT ->   "S";
            case PREDICATE -> "P";
            case OBJECT ->    "O";
            case GRAPH ->     "G";
            default -> throw new UndefinedCode(code);
        };
    }



}
