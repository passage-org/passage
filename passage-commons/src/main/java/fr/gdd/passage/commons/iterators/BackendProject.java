package fr.gdd.passage.commons.iterators;

import fr.gdd.passage.commons.factories.IBackendProjectsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendPullExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpProject;

import java.util.Iterator;

/**
 * Filter out the variables that are not projected.
 */
public class BackendProject<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    public static <ID,VALUE> IBackendProjectsFactory<ID,VALUE> factory() {
        return (context, input, op) -> {
            BackendPullExecutor<ID,VALUE> executor = context.getContext().get(BackendConstants.EXECUTOR);
            return new BackendProject<>(executor, op, input);
        };
    }

    final OpProject project;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final BackendPullExecutor<ID,VALUE> executor;

    BackendBindings<ID,VALUE> inputBinding;
    Iterator<BackendBindings<ID,VALUE>> instantiated = Iter.empty();

    public BackendProject(BackendPullExecutor<ID,VALUE> executor, OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        this.project = project;
        this.input = input;
        this.executor = executor;
    }

    @Override
    public boolean hasNext() {
        if (!instantiated.hasNext() && !input.hasNext())
            return false;

        while (!instantiated.hasNext() && input.hasNext()) {
            inputBinding = input.next();
            this.instantiated = executor.visit(project.getSubOp(), Iter.of(inputBinding));
        }

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return new BackendBindings<>(instantiated.next(), project.getVars()).setParent(inputBinding);
    }

}
