package fr.gdd.sage.rawer;

import org.apache.jena.sparql.util.Symbol;

public class RawerConstants {

    public static final String systemVarNS = "https://sage.gdd.fr/Rawer#";
    public static final String sageSymbolPrefix = "rawer";

    static public final Symbol BACKEND = allocConstantSymbol("Backend");
    static public final Symbol TIMEOUT = allocConstantSymbol("Timeout"); // max duration of execution
    static public final Symbol DEADLINE = allocConstantSymbol("Deadline"); // when to stop execution
    static public final Symbol LIMIT = allocConstantSymbol("Limit"); // max nb scans to perform
    static public final Symbol SCANS = allocVariableSymbol("Scans"); // nb of scan performed during execution

    static public final Symbol BUDGETING = allocVariableSymbol("Budgeting"); // distribute thresholds

    static public final Symbol CACHE = allocVariableSymbol("Cache"); // some kind of cache

    static public final Symbol SAVER = allocVariableSymbol("Saver"); // register the iterators needed

    static public final String COUNT_VARIABLE = "rawer_count"; // the name of the count variable for subqueries

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
