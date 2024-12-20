package fr.gdd.passage.commons.interfaces;
import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

/**
 * Not very different to classical Jena Accumulator except for input and
 * output types that must match the targeted backend.
 */
public interface BackendAccumulator<ID,VALUE> {

    void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv);

    VALUE getValue();

    default boolean gotIncremented() { throw new UnsupportedOperationException(); } // remove default

    /**
     * @return The value processed internally as a Java Double.
     */
    default double getValueAsDouble() { throw new UnsupportedOperationException();}

    /**
     * Aggregators can often be merged into one, e.g., min of min is min.
     * @param other The other BackendAccumulator to merge into this.
     */
    default void merge(BackendAccumulator<ID,VALUE> other) { throw new UnsupportedOperationException(); }

    default ExecutionContext getContext() {throw new UnsupportedOperationException(); } // TODO remove default when implem'd

}

