package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public interface IBackendValuesFactory<ID, VALUE> {

    /**
     * @param context The execution context that contains all the information
     *                needed to create the new iterator.
     * @param input The input iterator that provides mappings to instantiate.
     * @param op The values operator.
     * @return An iterator over the values and input.
     */
    Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context,
                                             Iterator<BackendBindings<ID, VALUE>> input,
                                             OpTable op);
}
