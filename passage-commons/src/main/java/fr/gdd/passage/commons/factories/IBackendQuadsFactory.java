package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public interface IBackendQuadsFactory<ID, VALUE> {

    /** The most default builder of iterator for scans over graphs (or context (or quads).
     * @param context The execution context that contains all the information
     *                needed to create the new scan.
     * @param input The input iterator that provides mappings to instantiate.
     * @param quad The quad operator.
     * @return An iterator over the quad pattern and input.
     */
    Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context,
                                             Iterator<BackendBindings<ID, VALUE>> input,
                                             OpQuad quad);

}
