package fr.gdd.passage.random.accumulators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.BackendAccumulator;
import fr.gdd.passage.volcano.PassageExecutionContext;
import org.apache.jena.sparql.function.FunctionEnv;

/**
 * Accumulate the information that enable approximate cardinality estimates.
 */
public class AccumulatorApproximateCount<ID,VALUE> implements BackendAccumulator<ID,VALUE> {

    final PassageExecutionContext<ID,VALUE> context;

    public AccumulatorApproximateCount(PassageExecutionContext<ID,VALUE> context) {
        this.context = context;
    }

    @Override
    public void accumulate(BackendBindings<ID, VALUE> binding, FunctionEnv functionEnv) {

    }

    @Override
    public VALUE getValue() {
        return null;
    }
}
