package fr.gdd.passage.volcano.iterators.union;

import fr.gdd.passage.commons.factories.IBackendUnionsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.pause.Pause2Next;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.Objects;

/**
 * Unions can be processed in parallel.
 */
public class PassageUnionParallelFactory<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendUnionsFactory<ID,VALUE> factory() {
        return (context, input, union) -> {
            Pause2Next<ID,VALUE> saver = context.getContext().get(PassageConstants.SAVER);
            Iterator<BackendBindings<ID,VALUE>> iterator = new PassageUnionParallelFactory<>(context, input, union);
            saver.register(union, iterator);
            return iterator;
        };
    }

    final ExecutionContext context;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final OpUnion union;
    Iterator<BackendBindings<ID,VALUE>> current;

    public PassageUnionParallelFactory(ExecutionContext context, Iterator<BackendBindings<ID, VALUE>> input, OpUnion union) {
        this.context = context;
        this.input = input;
        this.union = union;
    }

    @Override
    public boolean hasNext() {
        if (Objects.isNull(current) && !input.hasNext()) return false;

        if (Objects.nonNull(current) && current.hasNext()) return true;

        while (Objects.isNull(current) && input.hasNext()) {
            BackendBindings<ID, VALUE> inputBinding = input.next();
            current = new PassageUnionParallel<>(context, inputBinding, union);
            if (!current.hasNext()) {
                current = null;
            }
        }

        if (Objects.isNull(current)) return false;

        return current.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return current.next();
    }
}
