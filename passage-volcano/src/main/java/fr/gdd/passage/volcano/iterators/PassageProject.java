package fr.gdd.passage.volcano.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpProject;

import java.util.Iterator;

public class PassageProject<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final OpProject project;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final PassageOpExecutor<ID,VALUE> executor;

    BackendBindings<ID,VALUE> inputBinding;
    Iterator<BackendBindings<ID,VALUE>> instantiated = Iter.empty();

    public PassageProject(PassageOpExecutor<ID,VALUE> executor, OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
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

        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return new BackendBindings<>(instantiated.next(), project.getVars()).setParent(inputBinding);
    }

}
