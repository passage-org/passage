package fr.gdd.passage.volcano.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Iterator factory that enables configuring the behavior of each operator.
 * Still need templated ID, VALUE, and SKIP to ensure that iterator can access
 * to storage-dependant optimizations.
 */
public interface BackendIteratorFactory<ID,VALUE,SKIP extends Serializable> {

    /**
     * @param context The execution context must contain all information
     *                to instantiate the new iterator.
     * @param input The iterator providing new input to process to the current
     *              factory.
     * @param op The operator to create an iterator of.
     * @return A new iterator that take into account the incoming mappings and
     *  process them accordingly.
     */
    BackendIterator<ID,VALUE,SKIP> get(ExecutionContext context, Iterator<BackendBindings<ID,VALUE>> input, Op op);

}
