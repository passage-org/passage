package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.querypatterns.IsGroupByQuery;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpProject;

import java.util.stream.Stream;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;

public class PausableStreamProject<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PausableStream<ID,VALUE> wrapped;
    final PassagePushExecutor<ID,VALUE> executor;
    final OpProject project;
    final BackendBindings<ID,VALUE> input;

    public PausableStreamProject(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpProject project) {
        this.executor = (PassagePushExecutor<ID, VALUE>) context.executor;
        this.wrapped = executor.visit(project.getSubOp(), input); // check if could be a problem to inject the input in the subquery
        // this.wrapped = executor.visit(project.getSubOp(), context.bindingsFactory.get());
        this.project = project;
        this.input = input;
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        // TODO instead of copy, put a filter in the bindings
        return wrapped.stream().map(i -> new BackendBindings<>(i, project.getVars()).setParent(input));
    }

    @Override
    public Op pause() {
        Op subop = wrapped.pause();
        if (notExecuted(project.getSubOp(), subop)) return project;
        if (isDone(subop)) return DONE;

        // If the project of a COUNT GROUP BY clause, then it should be pushed down
        if (new IsGroupByQuery().visit(project) && subop instanceof OpJoin join) {
            Op inputToPushUp = join.getLeft();
            Op groupByToKeep = join.getRight();

            return OpJoin.create(inputToPushUp, OpCloningUtil.clone(project, groupByToKeep));
        }

        return OpCloningUtil.clone(project, subop);
    }
}
