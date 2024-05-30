package fr.gdd.sage.sager.iterators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerOpExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.op.OpGroup;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.aggregate.Aggregator;

import java.util.Iterator;

public class SagerAgg<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final SagerOpExecutor<ID,VALUE> executor;
    final OpGroup op;
    final Iterator<BackendBindings<ID,VALUE>> input;
    final ExecutionContext context;

    public SagerAgg (SagerOpExecutor<ID, VALUE> executor, OpGroup op, Iterator<BackendBindings<ID,VALUE>> input, ExecutionContext context){
        this.executor = executor;
        this.op = op;
        this.input = input;
        this.context = context;
    }

    @Override
    public boolean hasNext() {
        return this.input.hasNext();
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        Iterator<BackendBindings<ID,VALUE>> subop = ReturningArgsOpVisitorRouter.visit(executor, op.getSubOp(), Iter.of(input.next()));

        while (subop.hasNext()) {
            BackendBindings<ID,VALUE> bindings = subop.next();
            // TODO see QueryIterGroup
        }
        throw new UnsupportedOperationException("TODO"); // TODO TODO TODO
    }
}
