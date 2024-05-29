package fr.gdd.sage.sager.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpProject;

import java.util.Iterator;

public class SagerProject<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final OpProject project;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final SagerOpExecutor<ID,VALUE> executor;

    BackendBindings<ID,VALUE> inputBinding;
    Iterator<BackendBindings<ID,VALUE>> instantiated = Iter.empty();

    public SagerProject(SagerOpExecutor<ID,VALUE> executor, OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
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
            this.instantiated = ReturningArgsOpVisitorRouter.visit(executor, project.getSubOp(), Iter.of(inputBinding));
        }

        return true;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return new BackendBindings<>(instantiated.next(), project.getVars()).setParent(inputBinding);
    }

}
