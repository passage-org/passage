package fr.gdd.passage.volcano;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.CacheId;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.iterators.BackendBind;
import fr.gdd.passage.volcano.iterators.PassageScanFactory;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;

/**
 * Executor dedicated to sub-queries that are preempted. For now, these are as simple
 * as a triple pattern, with "BIND … AS …" to create its context.
 * It does nothing extraordinary, but it checks if the subquery is
 * using an operator that is not be supported.
 */
public class PassageSubOpExecutor<ID,VALUE> extends ReturningArgsOpVisitor<
        Iterator<BackendBindings<ID, VALUE>>, // input
        Iterator<BackendBindings<ID, VALUE>>> { // output

    final ExecutionContext execCxt;
    final Backend<ID, VALUE, Long> backend;
    final CacheId<ID,VALUE> cache;

    Long skipTo = null;

    public PassageSubOpExecutor(ExecutionContext execCxt, Backend<ID,VALUE,Long> backend) {
        this.execCxt = execCxt;
        this.backend = backend;
        this.cache = execCxt.getContext().get(PassageConstants.CACHE);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpSlice slice, Iterator<BackendBindings<ID, VALUE>> input) {
        skipTo = slice.getStart();
        return ReturningArgsOpVisitorRouter.visit(this, slice.getSubOp(), input);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpExtend extend, Iterator<BackendBindings<ID,VALUE>> input) {
        Iterator<BackendBindings<ID,VALUE>> newInput = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
        return new BackendBind<>(newInput, extend, backend, cache, execCxt);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpJoin join, Iterator<BackendBindings<ID,VALUE>> input) {
        input = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), input);
        return ReturningArgsOpVisitorRouter.visit(this, join.getRight(), input);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple opTriple, Iterator<BackendBindings<ID, VALUE>> input) {
        return new PassageScanFactory<>(input, execCxt, opTriple, skipTo);
    }

    @Override
    public Iterator<BackendBindings<ID,VALUE>> visit(OpTable table, Iterator<BackendBindings<ID,VALUE>> input) {
        if (table.isJoinIdentity())
            return input;
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        // TODO double check projected variables.
        return ReturningArgsOpVisitorRouter.visit(this, project.getSubOp(), input);
    }
}
