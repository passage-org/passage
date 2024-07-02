package fr.gdd.sage.rawer;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.budgeting.NaiveBudgeting;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.iterators.SagerBind;
import fr.gdd.sage.rawer.iterators.ProjectIterator;
import fr.gdd.sage.rawer.iterators.RandomRoot;
import fr.gdd.sage.rawer.iterators.RandomScanFactory;
import fr.gdd.sage.rawer.iterators.RawerAgg;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.pause.Save2SPARQL;
import fr.gdd.sage.sager.resume.BGP2Triples;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.aggregate.AggCount;
import org.apache.jena.sparql.expr.aggregate.AggCountDistinct;
import org.apache.jena.sparql.expr.aggregate.AggCountVar;
import org.apache.jena.sparql.expr.aggregate.AggCountVarDistinct;

import java.util.Iterator;

/**
 * Execute the query and exactly the query that has been asked.
 * If an operator is not implemented, then it returns the explicit mention
 * that it's not implemented. No surprises.
 */
public class RawerOpExecutor<ID, VALUE> extends ReturningArgsOpVisitor<
        Iterator<BackendBindings<ID, VALUE>>, // input
        Iterator<BackendBindings<ID, VALUE>>> { // output

    final ExecutionContext execCxt;
    final Backend<ID, VALUE, ?> backend;

    public RawerOpExecutor(ExecutionContext execCxt) {
        this.execCxt = execCxt;
        this.backend = execCxt.getContext().get(RawerConstants.BACKEND);
        execCxt.getContext().setIfUndef(RawerConstants.SCANS, 0L);
        execCxt.getContext().setIfUndef(RawerConstants.LIMIT, Long.MAX_VALUE);
        execCxt.getContext().setIfUndef(RawerConstants.TIMEOUT, Long.MAX_VALUE);
    }

    public RawerOpExecutor<ID, VALUE> setTimeout(Long timeout) {
        execCxt.getContext().set(RawerConstants.TIMEOUT, timeout);
        execCxt.getContext().set(RawerConstants.DEADLINE, System.currentTimeMillis()+timeout);
        return this;
    }

    public RawerOpExecutor<ID, VALUE> setLimit(Long limit) {
        execCxt.getContext().set(RawerConstants.LIMIT, limit);
        return this;
    }

    public Backend<ID, VALUE, ?> getBackend() {
        return backend;
    }

    public ExecutionContext getExecutionContext() {
        return execCxt;
    }

    /* ************************************************************************ */

    public Iterator<BackendBindings<ID, VALUE>> execute(Op root) {
        execCxt.getContext().setIfUndef(RawerConstants.BUDGETING, new NaiveBudgeting(
                        execCxt.getContext().get(RawerConstants.TIMEOUT),
                        execCxt.getContext().get(RawerConstants.LIMIT)));
        root = ReturningOpVisitorRouter.visit(new BGP2Triples(), root); // TODO fix
        execCxt.getContext().set(SagerConstants.SAVER, new Save2SPARQL<>(root, execCxt));
        return new RandomRoot<>(this, execCxt, root);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTriple triple, Iterator<BackendBindings<ID, VALUE>> input) {
        return new RandomScanFactory<>(input, execCxt, triple);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpProject project, Iterator<BackendBindings<ID, VALUE>> input) {
        return new ProjectIterator<>(project, ReturningArgsOpVisitorRouter.visit(this, project.getSubOp(), input));
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpExtend extend, Iterator<BackendBindings<ID, VALUE>> input) {
        // TODO throw when the expressions inside the OpExtend are not supported
        Iterator<BackendBindings<ID, VALUE>> wrapped = ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), input);
        return new SagerBind<>(wrapped, extend, backend, execCxt);
    }


    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpJoin join, Iterator<BackendBindings<ID, VALUE>> input) {
        input = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), input);
        return ReturningArgsOpVisitorRouter.visit(this, join.getRight(), input);
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpTable table, Iterator<BackendBindings<ID, VALUE>> input) {
        if (table.isJoinIdentity())
            return input;
        throw new UnsupportedOperationException("TODO: VALUES…"); // TODO
    }

    @Override
    public Iterator<BackendBindings<ID, VALUE>> visit(OpGroup groupBy, Iterator<BackendBindings<ID, VALUE>> input) {
        // TODO make it budget-based
        long limit = execCxt.getContext().getLong(RawerConstants.LIMIT, 0L);
        if (limit <= 0L) {
            return input;
        }

        // execCxt.getContext().set(RawerConstants.LIMIT, (long) limit/2);
        for (int i = 0; i < groupBy.getAggregators().size(); ++i) {
            switch (groupBy.getAggregators().get(i).getAggregator()) {
                case AggCount ac -> {} // nothing, just checking it's handled (this is COUNT(*))
                // case AggCountVar acv -> {} // TODO count when (a) variable(s) is/are bound
                case AggCountVarDistinct acvd -> {}
                // case AggCountDistinct acd -> {} // nothing
                default -> throw new UnsupportedOperationException("The aggregation function is not implemented: " +
                        groupBy.getAggregators().get(i).toString());
            }
        }

        return new RawerAgg<>(this, groupBy, input);
    }

}
