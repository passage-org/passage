package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public interface IBackendCountsFactory<ID, VALUE> {

    /** Iterator for aggregate COUNT queries.
     * @param context The execution context that contains all the information
     *                needed to create the new scan.
     * @param input The input iterator that provides mappings to instantiate.
     * @param agg The aggregate operator for COUNT.
     * @return An iterator over the aggregate.
     */
    Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context,
                                             Iterator<BackendBindings<ID, VALUE>> input,
                                             OpGroup agg);
}
