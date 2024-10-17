package fr.gdd.passage.volcano;

import org.apache.jena.sparql.util.Symbol;

public class PassageConstants {

    public static final String systemVarNS = "https://passage.gdd.fr/Passage#";
    public static final String sageSymbolPrefix = "passage";

    static public final Symbol LOADER = allocVariableSymbol("Loader");
    static public final Symbol SAVER = allocVariableSymbol("Saver");

    static public final Symbol TIMEOUT = allocConstantSymbol("Timeout"); // max duration of execution
    static public final Symbol DEADLINE = allocConstantSymbol("Deadline"); // when to stop execution
    static public final Symbol SCANS = allocVariableSymbol("Scans"); // current number of scans
    static public final Symbol LIMIT = allocConstantSymbol("Limit"); // max number of scans

    static public final Symbol OFFSET = allocConstantSymbol("Offset"); // The offset of the subquery

    static public final Symbol PAUSED = allocVariableSymbol("Paused"); // is the execution paused/stopped.
    static public final Symbol PAUSED_STATE = allocVariableSymbol("PausedState"); // The returned SPARQL query of the paused state.

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
        return Symbol.create(sageSymbolPrefix + name);
    }
}
