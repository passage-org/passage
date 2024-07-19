package fr.gdd.sage.rawer;

import org.apache.jena.sparql.engine.ExecutionContext;
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

    // There are multiples implementation of the count distinct accumulator, so which
    // one should we use?
    static public final Symbol COUNT_DISTINCT_FACTORY = allocVariableSymbol("CountDistinctFactory");

    static public final Symbol MAX_THREADS = allocConstantSymbol("MaxThread");

    static public final Symbol FORCE_ORDER = allocConstantSymbol("ForceOrder");

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

    /**
     * Increment by 1 the number of scans, i.e., call to scan's `next()`.
     * @param context The execution context of the query.
     */
    public static void incrementScans(ExecutionContext context) {
        context.getContext().set(RawerConstants.SCANS, getScans(context) + 1);
    }

    /**
     * Increment the number of scans, i.e., call to scan's `next()`. Byt the number of scans
     * done in the `other` execution context.
     * @param context The execution context of the query.
     * @param other The execution context of the subquery.
     */
    public static void incrementScansBy(ExecutionContext context, ExecutionContext other) {
        long nbScansSubQuery = getScans(other);
        context.getContext().set(RawerConstants.SCANS, getScans(context) + nbScansSubQuery);
    }

    /**
     * @param context The context to look into.
     * @return The number of scans in the context.
     */
    public static long getScans(ExecutionContext context) {
        return context.getContext().getLong(RawerConstants.SCANS, 0L);
    }
}
