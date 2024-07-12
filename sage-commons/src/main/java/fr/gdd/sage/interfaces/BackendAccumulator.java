package fr.gdd.sage.interfaces;
import fr.gdd.sage.generics.BackendBindings;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

/**
 * Not very different to classical Jena Accumulator except for input and
 * output types that must match the targeted backend.
 */
public interface BackendAccumulator<ID,VALUE> {

    void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv);

    VALUE getValue();

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

