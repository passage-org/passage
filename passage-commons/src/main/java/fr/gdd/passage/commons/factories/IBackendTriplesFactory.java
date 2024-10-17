package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

public interface IBackendTriplesFactory<ID, VALUE> {

    /** The most default builder of iterator for scans.
     * @param context The execution context that contains all the information
     *                needed to create the new scan.
     * @param input The input iterator that provides mappings to instantiate.
     * @param op The triple operator.
     * @return An iterator over the triple pattern and input.
     */
    Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context,
                                             Iterator<BackendBindings<ID, VALUE>> input,
                                             OpTriple op);

}
