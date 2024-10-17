package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public interface IBackendProjectsFactory<ID, VALUE> {

    /** The most default builder of iterator for projections.
     * @param context The execution context that contains all the information
     *                needed to create the new scan.
     * @param input The input iterator that provides mappings to instantiate.
     * @param op The project operator.
     * @return An iterator over the projection and input.
     */
    Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context,
                                             Iterator<BackendBindings<ID, VALUE>> input,
                                             OpProject op);
}
