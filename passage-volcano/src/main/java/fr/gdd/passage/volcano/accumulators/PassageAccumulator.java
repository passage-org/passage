package fr.gdd.passage.volcano.accumulators;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.function.FunctionEnv;

public interface PassageAccumulator<ID,VALUE> {
    void accumulate(BackendBindings<ID,VALUE> binding, FunctionEnv functionEnv);

    VALUE getValue();
}
