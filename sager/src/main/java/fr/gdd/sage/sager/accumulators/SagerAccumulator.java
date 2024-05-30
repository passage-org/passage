package fr.gdd.sage.sager.accumulators;

import fr.gdd.sage.generics.BackendBindings;
import org.apache.jena.sparql.function.FunctionEnv;

public interface SagerAccumulator<ID,VALUE> {
    void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv);

    VALUE getValue();
}
