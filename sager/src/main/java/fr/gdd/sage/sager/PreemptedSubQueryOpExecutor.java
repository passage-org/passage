package fr.gdd.sage.sager;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.iterators.SagerBind;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

/**
 * Executor dedicated to sub-queries that are preempted. For now, these are as simple
 * as a triple pattern, with "BIND … AS …" to create its context.
 * @param <ID>
 * @param <VALUE>
 */
public class PreemptedSubQueryOpExecutor<ID,VALUE> extends ReturningArgsOpVisitor<
        Iterator<BackendBindings<ID, VALUE>>, // input
        Iterator<BackendBindings<ID, VALUE>>> { // output

    final ExecutionContext execCxt;
    final Backend<ID, VALUE, Long> backend;

    Long skipTo = null;

    public PreemptedSubQueryOpExecutor(ExecutionContext execCxt, Backend<ID,VALUE,Long> backend) {
        this.execCxt = execCxt;
        this.backend = backend;
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpSlice slice, Iterator<BackendBindings<ID, VALUE>> input) {
        skipTo = slice.getStart();
        return ReturningArgsOpVisitorRouter.visit(this, slice.getSubOp(), input);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpExtend extend, Iterator<BackendBindings<ID,VALUE>> input) {
        Iterator<BackendBindings<ID,VALUE>> newInput = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
        return new SagerBind<>(newInput, extend, execCxt);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpJoin join, Iterator<BackendBindings<ID,VALUE>> input) {
        input = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), input);
        return ReturningArgsOpVisitorRouter.visit(this, join.getRight(), input);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple opTriple, Iterator<BackendBindings<ID, VALUE>> input) {
        return new SagerScanFactory<>(input, execCxt, opTriple, skipTo);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpTable table, Iterator<BackendBindings<ID,VALUE>> input) {
        if (table.isJoinIdentity())
            return input;
        throw new UnsupportedOperationException();
    }

}
