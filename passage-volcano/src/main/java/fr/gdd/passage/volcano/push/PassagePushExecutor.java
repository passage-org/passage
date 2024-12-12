package fr.gdd.passage.volcano.push;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutor;
import fr.gdd.passage.volcano.exceptions.PauseException;
import fr.gdd.passage.volcano.push.streams.PassagePushLimitOffset;
import fr.gdd.passage.volcano.push.streams.PassagePushOptional;
import fr.gdd.passage.volcano.push.streams.PassagePushValues;
import fr.gdd.passage.volcano.push.streams.PassageSplitScan;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;

import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PassagePushExecutor<ID,VALUE> extends ReturningArgsOpVisitor<
        Stream<BackendBindings<ID, VALUE>>, // output
        BackendBindings<ID, VALUE>> implements PassageExecutor<ID,VALUE> { // input

    final PassageExecutionContext<ID,VALUE> context;

    public PassagePushExecutor (PassageExecutionContext<ID,VALUE> context) {
        this.context = context;
        this.context.getContext().set(BackendConstants.EXECUTOR, this); // TODO, let a super do the job
    }

    @Override
    public Op execute(String query, Consumer<BackendBindings<ID, VALUE>> consumer) {
        return this.execute(Algebra.compile(QueryFactory.create(query)), consumer);
    }

    @Override
    public Op execute(Op root, Consumer<BackendBindings<ID, VALUE>> consumer) {
        root = context.optimizer.optimize(root);
        final Op _root = new DefaultGraphUriQueryModifier(context).visit(root);
        AtomicBoolean gotPaused = new AtomicBoolean(false);
        try (ForkJoinPool customPool = new ForkJoinPool(context.maxParallelism)) {
            customPool.submit(() -> {
                try {
                    this.visit(_root, new BackendBindings<>()).forEach(consumer);
                } catch (PauseException pe) {
                    gotPaused.set(true);
                }
            }).join();
        }
        return gotPaused.get() ?
                new Pause2Continuation<>(context.op2its).get(_root):
                null; // null means execution is over: we provided complete and correct results
    }

    /* *********************************** OPERATORS ************************************* */

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpTriple triple, BackendBindings<ID, VALUE> input) {
        return StreamSupport.stream(new PassageSplitScan<>(context, input, triple), this.context.maxParallelism > 1);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpQuad quad, BackendBindings<ID, VALUE> input) {
        return StreamSupport.stream(new PassageSplitScan<>(context, input, quad), this.context.maxParallelism > 1);
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpJoin join, BackendBindings<ID, VALUE> input) {
         return this.visit(join.getLeft(), input).flatMap(b -> this.visit(join.getRight(), b));
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpUnion union, BackendBindings<ID, VALUE> input) {
        return Stream.concat(this.visit(union.getLeft(), input), this.visit(union.getRight(), input));
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpExtend extend, BackendBindings<ID, VALUE> input) {
        return this.visit(extend.getSubOp(), input).map(i -> {
            BackendBindings<ID, VALUE> b = new BackendBindings<ID, VALUE>().setParent(i);
            for (Var v : extend.getVarExprList().getVars()) {
                b.put(v, new BackendBindings.IdValueBackend<ID, VALUE>()
                        .setBackend(context.backend)
                        .setString(NodeFmtLib.strNT(extend.getVarExprList().getExpr(v)
                                .eval(i, context).asNode())));
            }
            return b;
        });
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpTable table, BackendBindings<ID, VALUE> input) {
        return new PassagePushValues<>(context, input, table).getStream();
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpSlice slice, BackendBindings<ID, VALUE> input) {
        return new PassagePushLimitOffset<>(context, input, slice).stream();
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpProject project, BackendBindings<ID, VALUE> input) {
        return this.visit(project.getSubOp(), input).map(i -> new BackendBindings<>(i, project.getVars()));
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpFilter filter, BackendBindings<ID, VALUE> input) {
        return this.visit(filter.getSubOp(), input).filter(i -> filter.getExprs().isSatisfied(i, context));
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpLeftJoin lj, BackendBindings<ID, VALUE> input) {
        if (Objects.nonNull(lj.getExprs()) && !lj.getExprs().isEmpty()) {
            throw new UnsupportedOperationException("Conditions in left joins are not handled yet.");
        }
        return new PassagePushOptional<>(context, input, lj).stream();
    }
}
