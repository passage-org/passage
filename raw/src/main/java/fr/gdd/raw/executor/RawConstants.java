package fr.gdd.raw.executor;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.Symbol;

public class RawConstants {

    public static final String systemVarNS = "https://sage.gdd.fr/Raw#";
    public static final String sageSymbolPrefix = "raw";

    static public final Symbol BACKEND = allocConstantSymbol("Backend");
    static public final Symbol TIMEOUT = allocConstantSymbol("Timeout"); // max duration of execution
    static public final Symbol DEADLINE = allocConstantSymbol("Deadline"); // when to stop execution
    static public final Symbol LIMIT = allocConstantSymbol("Limit"); // max nb scans to perform
    static public final Symbol SCANS = allocVariableSymbol("Scans"); // nb of scan performed during execution
    static public final Symbol RANDOM_WALK_ATTEMPTS = allocConstantSymbol("RandomWalkAttempts");
    static public final Symbol ATTEMPT_LIMIT = allocConstantSymbol("AttemptLimit");

    static public final Symbol BUDGETING = allocVariableSymbol("Budgeting"); // distribute thresholds

    static public final Symbol CACHE = allocVariableSymbol("Cache"); // some kind of cache

    static public final Symbol SAVER = allocVariableSymbol("Saver"); // register the iterators needed

    static public final String COUNT_VARIABLE = "rawer_count"; // the name of the count variable for subqueries

    // There are multiples implementation of the count distinct accumulator, so which
    // one should we use?
    static public final Symbol COUNT_DISTINCT_FACTORY = allocVariableSymbol("CountDistinctFactory");

    static public final Symbol MAX_THREADS = allocConstantSymbol("MaxThread");

    static public final Symbol FORCE_ORDER = allocConstantSymbol("ForceOrder");

    static public final Var MAPPING_PROBABILITY = Var.alloc("probabilityOfRetrievingRestOfMapping");

    static public final String VAR_PROBABILITY_PREFIX = "probabilityOfRetrievingVariable_";

    static public final Var RANDOM_WALK_HOLDER = Var.alloc("randomWalkHolder");

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
        context.getContext().set(RawConstants.SCANS, getScans(context) + 1);
    }

    /**
     * Increment the number of scans, i.e., call to scan's `next()`. Byt the number of scans
     * done in the `other` execution context.
     * @param context The execution context of the query.
     * @param other The execution context of the subquery.
     */
    public static void incrementScansBy(ExecutionContext context, ExecutionContext other) {
        long nbScansSubQuery = getScans(other);
        context.getContext().set(RawConstants.SCANS, getScans(context) + nbScansSubQuery);
    }

    /**
     * @param context The context to look into.
     * @return The number of scans in the context.
     */
    public static long getScans(ExecutionContext context) {
        return context.getContext().getLong(RawConstants.SCANS, 0L);
    }

    public static void setRandomWalkAttemps(ExecutionContext executionContext, Long numberOfAttempts) {
        ((Wrapper) executionContext.getContext().get(RawConstants.RANDOM_WALK_ATTEMPTS)).setValue(numberOfAttempts);
    }

    public static Long getRandomWalkAttempts(ExecutionContext executionContext) {
        return ((Wrapper<Long>) executionContext.getContext().get(RawConstants.RANDOM_WALK_ATTEMPTS)).getValue();
    }

    public static Long getRandomWalkAttempts(Context context) {
        return ((Wrapper<Long>) context.get(RawConstants.RANDOM_WALK_ATTEMPTS)).getValue();
    }

    public static Long incrementRandomWalkAttempts(ExecutionContext executionContext) {
        setRandomWalkAttemps(executionContext, getRandomWalkAttempts(executionContext) + 1L);
        return getRandomWalkAttempts(executionContext);
    }

    // For some reason, the context does not properly save changes to objects like Longs throughout query execution,
    // as updating (incrementing) a Long generates a new reference. Using a wrapper ensures the updated object remains
    // the same during the entire execution (since the reference to the wrapper itself isn't changed, like a pointer)

    static class Wrapper<T>{
        T value;
        Wrapper(T value) {
            this.value = value;
        }
        public T getValue() {
            return value;
        }
        public void setValue(T value) {
            this.value = value;
        }
    }
}
