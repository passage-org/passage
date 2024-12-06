package fr.gdd.passage.volcano.push;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.transforms.DefaultGraphUriQueryModifier;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.pause.PauseException;
import fr.gdd.passage.volcano.push.streams.PassagePushLimitOffset;
import fr.gdd.passage.volcano.push.streams.PassagePushValues;
import fr.gdd.passage.volcano.push.streams.PassageSplitScan;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PassagePushExecutor<ID,VALUE> extends ReturningArgsOpVisitor<
        Stream<BackendBindings<ID, VALUE>>, // output
        BackendBindings<ID, VALUE>> { // input

    final PassageExecutionContext<ID,VALUE> context;

    public PassagePushExecutor (PassageExecutionContext<ID,VALUE> context) {
        this.context = context;
        this.context.getContext().set(BackendConstants.EXECUTOR, this); // TODO, let a super do the job
    }

    @Deprecated // should not be used
    public Stream<BackendBindings<ID, VALUE>> execute(Op root) {
        root = context.optimizer.optimize(root);
        root = new DefaultGraphUriQueryModifier(context).visit(root);
        context.setQuery(root); // mandatory to be saved later on
        return this.visit(root, new BackendBindings<>());
    }

    public Op execute(Op root, Consumer<BackendBindings<ID, VALUE>> consumer) {
        root = context.optimizer.optimize(root);
        final Op _root = new DefaultGraphUriQueryModifier(context).visit(root);
        context.setQuery(root); // mandatory to be saved later on
        // With a timeout condition, we need to create a catch that wraps the
        // whole process.
        try (ForkJoinPool customPool = new ForkJoinPool(context.maxParallelism)) {
            customPool.submit(() -> {
                try {
                    this.visit(_root, new BackendBindings<>()).forEach(consumer);
                } catch (PauseException pe) {
                    // TODO test if there are multiple PauseException catch.
                    //      The best scenario would be that children continue
                    //      their execution until throwing. So when join is called
                    //      they are all stopped in a consistent state.
                    System.out.println("Stop !"); // TODO remove this stop
                }
            }).join();
        }
        return new Pause2ContinuationQuery<>(context.op2its).visit(_root);
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
                Expr expr = extend.getVarExprList().getExpr(v);
                BackendBindings.IdValueBackend<ID, VALUE> newBinding = new BackendBindings.IdValueBackend<ID, VALUE>()
                        .setBackend(context.backend);
                NodeValue newValue = expr.eval(i, context);
                newBinding.setString(NodeFmtLib.strNT(newValue.asNode()));
                b.put(v, newBinding);
            }
            return b;
        });
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> visit(OpTable table, BackendBindings<ID, VALUE> input) {
        if (table.isJoinIdentity()) {
            return Stream.of(input);
        }
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
        return this.visit(filter.getSubOp(), input).filter(i -> filter.getExprs().isSatisfied(i, this.context));
    }
}
