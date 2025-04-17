package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.streams.IWrappedStream;
import org.apache.jena.sparql.engine.ExecutionContext;

/**
 * General purpose operator factory.
 */
public interface IBackendOperatorFactory<ID,VALUE,OP> {

    IWrappedStream<BackendBindings<ID,VALUE>> get(ExecutionContext context, BackendBindings<ID, VALUE> input, OP op);

}
