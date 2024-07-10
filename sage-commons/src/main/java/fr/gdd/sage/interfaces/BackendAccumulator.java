package fr.gdd.sage.interfaces;
import fr.gdd.sage.generics.BackendBindings;
import org.apache.jena.sparql.function.FunctionEnv;

/**
 * Not very different to classical Jena Accumulator except for input and
 * output types that must match the targeted backend.
 */
public interface BackendAccumulator<ID,VALUE> {

    void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv);

    VALUE getValue();

}

