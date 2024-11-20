package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public interface IBackendServicesFactory<ID,VALUE> {

    /** Builder of iterator for services.
     * @param context The execution context that contains all the information
     *                needed to create the new iterator over a service operator.
     * @param input The input iterator that provides mappings to instantiate.
     * @param service The service operator.
     * @return An iterator over the service clause.
     */
    Iterator<BackendBindings<ID,VALUE>> get(ExecutionContext context,
                                  Iterator<BackendBindings<ID,VALUE>> input,
                                  OpService service);
}
