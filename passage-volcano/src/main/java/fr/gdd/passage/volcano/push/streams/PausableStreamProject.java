package fr.gdd.passage.volcano.push.streams;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.push.PassagePushExecutor2;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;

import java.util.stream.Stream;

import static fr.gdd.passage.volcano.push.Pause2Continuation.*;

public class PausableStreamProject<ID,VALUE> implements PausableStream<ID,VALUE> {

    final PausableStream<ID,VALUE> wrapped;
    final PassagePushExecutor2<ID,VALUE> executor;
    final OpProject project;

    public PausableStreamProject(PassageExecutionContext<ID,VALUE> context, BackendBindings<ID,VALUE> input, OpProject project) {
        this.executor = (PassagePushExecutor2<ID, VALUE>) context.executor;
        this.wrapped = executor.visit(project.getSubOp(), input);
        this.project = project;
    }

    @Override
    public Stream<BackendBindings<ID, VALUE>> stream() {
        return wrapped.stream().map(i -> new BackendBindings<>(i, project.getVars()));
    }

    @Override
    public Op pause() {
        Op subop = wrapped.pause();
        if (notExecuted(project.getSubOp(), subop)) return project;
        if (isDone(subop)) return DONE;
        return OpCloningUtil.clone(project, subop);
    }
}
