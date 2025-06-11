package fr.gdd.passage.commons.generics;

import org.apache.jena.sparql.util.Symbol;

public class BackendConstants {

    public static final String systemVarNS = "https://passage.gdd.fr/Common#";
    public static final String passageSymbolPrefix = "common";

    static public final Symbol DESCRIPTION = allocVariableSymbol("Description"); // the model describing the endpoint
    static public final Symbol EXECUTOR = allocConstantSymbol("Executor"); // The current executor executing
    static public final Symbol EXECUTOR_FACTORY = allocConstantSymbol("ExecutorFactory"); // Create executors
    static public final Symbol BACKEND = allocConstantSymbol("Backend"); // The backend in question
    static public final Symbol CACHE = allocVariableSymbol("Cache"); // some kind of cache

    /* ********************************************************************** */

    /**
     * Symbol in use in the global context.
     */
    public static Symbol allocConstantSymbol(String name) {
        return Symbol.create(systemVarNS + name);
    }

    /**
     * Symbol in use in each execution context.
     */
    public static Symbol allocVariableSymbol(String name) {
        return Symbol.create(passageSymbolPrefix + name);
    }
}
