package fr.gdd.raw.budgeting;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

/**
 * A budgeting interface that allows operator to run multiple
 * times instead of one within the limits prescribed. In other terms,
 * how many times will the operator call `hasNext` for its downstream
 * operators.
 * This proves useful
 * when the operator is an aggregator that needs multiple insights from
 * downstream operators.
 */
public interface IBudgeting<OUT> {

    /**
     * @param input The mappings processed up till then.
     * @param op The operator that needs a budget.
     * @param context The execution context of the whole query.
     * @return The threshold allowed.
     */
    <ID, VALUE> OUT get(BackendBindings<ID, VALUE> input, Op op, ExecutionContext context);

}
