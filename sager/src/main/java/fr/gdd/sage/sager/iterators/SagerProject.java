package fr.gdd.sage.sager.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerOpExecutor;
import org.apache.jena.sparql.algebra.op.OpProject;

import java.util.Iterator;

public class SagerProject<ID,VALUE> implements Iterator<BackendBindings<ID, VALUE>> {

    final OpProject project;
    final Iterator<BackendBindings<ID,VALUE>> instantiated;

    public SagerProject(SagerOpExecutor<ID,VALUE> executor, OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        this.project = project;
        this.instantiated = ReturningArgsOpVisitorRouter.visit(executor, project.getSubOp(), input);
    }

    @Override
    public boolean hasNext() {
        return instantiated.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        return new BackendBindings<>(instantiated.next(), project.getVars());
    }

}
