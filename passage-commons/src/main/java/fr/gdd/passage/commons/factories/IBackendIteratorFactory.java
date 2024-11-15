package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public interface IBackendIteratorFactory<ID,VALUE,OP> {

    /**
     * @param context The execution context that contains all the information
     *                needed to create the new iterator.
     * @param input The environment mapping that constitutes the input of the operator.
     * @param op The typed operator.
     * @return An iterator corresponding to the OP.
     */
    Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context,
                                             BackendBindings<ID, VALUE> input,
                                             OP op);
}
