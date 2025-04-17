package fr.gdd.passage.commons.factories;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Create iterators for BIND clauses.
 */
public interface IBackendBindsFactory<ID, VALUE> {

    /**
     * @param context The execution context that contains all the information
     *                needed to create the new iterator.
     * @param input The input iterator that provides mappings to instantiate.
     * @param op The bind as operator.
     * @return An iterator over the bind as and input.
     */
    Iterator<BackendBindings<ID, VALUE>> get(ExecutionContext context,
                                             Iterator<BackendBindings<ID, VALUE>> input,
                                             OpExtend op);

    default Stream<BackendBindings<ID,VALUE>> get(ExecutionContext context,
                                                  BackendBindings<ID, VALUE> input,
                                                  OpExtend op) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(get(context, List.of(input).iterator(), op), 0),
                false);
    }
}
