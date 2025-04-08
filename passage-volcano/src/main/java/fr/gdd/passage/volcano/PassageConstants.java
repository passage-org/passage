package fr.gdd.passage.volcano;

import org.apache.jena.sparql.util.Symbol;

public class PassageConstants {

    public static final String systemVarNS = "https://passage.gdd.fr/Passage#";
    public static final String sageSymbolPrefix = "passage";

    static public final Symbol LOADER = allocVariableSymbol("Loader");
    @Deprecated
    static public final Symbol SAVER = allocVariableSymbol("Saver");

    static public final Symbol MAX_PARALLELISM = allocConstantSymbol("MaxParallelism"); // the thread pool size for the execution stream

    static public final Symbol TIMEOUT = allocConstantSymbol("Timeout"); // max duration of execution
    static public final Symbol DEADLINE = allocConstantSymbol("Deadline"); // when to stop execution
    static public final Symbol SCANS = allocVariableSymbol("Scans"); // current number of scans
    static public final Symbol MAX_SCANS = allocConstantSymbol("MaxScans"); // max number of scans
    static public final Symbol RESULTS = allocConstantSymbol("Results"); // number of results
    static public final Symbol MAX_RESULTS = allocConstantSymbol("MaxResults"); // max number of results whatever the query
    static public final Symbol SERVICE_CALLS = allocVariableSymbol("ServiceCalls"); // number of service calls performed

    static public final Symbol LIMIT = allocConstantSymbol("Limit"); // The limit of the subquery
    static public final Symbol OFFSET = allocConstantSymbol("Offset"); // The offset of the subquery
    static public final Symbol PROJECT = allocConstantSymbol("Project"); // The projected values of the subquery

    static public final Symbol PAUSED = allocVariableSymbol("Paused"); // is the execution paused/stopped.

    static public final Symbol FORCE_ORDER = allocConstantSymbol("ForceOrder");
    static public final Symbol BACKJUMP = allocConstantSymbol("Backjump");
    static public final Symbol SPLIT_SCANS = allocConstantSymbol("SplitScans"); // for parallel streams
    static public final Symbol STOPPING_CONDITIONS = allocConstantSymbol("StoppingCondition");

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
