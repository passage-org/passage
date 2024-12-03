package fr.gdd.passage.volcano.pull.iterators;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.pull.PassagePullExecutor;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

/**
 * An iterator that creates two child threads, one for each side
 * of the union operator.
 */
public class PassageUnionParallel<ID,VALUE> implements Iterator<BackendBindings<ID,VALUE>> {

    final ExecutionContext context;
    final OpUnion union;
    final BackendBindings<ID,VALUE> input;
    final ConcurrentLinkedQueue<BackendBindings<ID, VALUE>> produced = new ConcurrentLinkedQueue<>(); // TODO max capacity
    // TODO Future with paused plan maybe.
    Set<Future<Void>> workers = Collections.synchronizedSet(new HashSet<>()); // no need for big efficiency, should do the job
    boolean started = false;

    static ExecutorService executorService = Executors.newFixedThreadPool(10); // TODO put it in context

    public PassageUnionParallel(ExecutionContext context, BackendBindings<ID,VALUE> input, OpUnion union) {
        this.input = input;
        this.union = union;
        this.context = context;

        workers.add(createWorker(union.getLeft()));
        workers.add(createWorker(union.getRight()));
    }

    @Override
    public boolean hasNext() {
        if (!started) {
            // TODO TODO TODO
            // threads.forEach(Thread::start);
            started = true;
        }

        while (!workers.isEmpty()) {
            if (!produced.isEmpty()) return true;
        }

        return false;
    }

    @Override
    public BackendBindings<ID, VALUE> next() {
        BackendBindings<ID,VALUE> toConsume = produced.poll();
        return toConsume.setParent(input); // never null since hasNext was true
    }

    /**
     * @param subOp The subOperator to execute.
     * @return The thread dedicated to execute the operator.
     */
    private Future<Void> createWorker(Op subOp) {

        CompletableFuture<Void> worker = CompletableFuture.runAsync(() -> {
            PassageExecutionContext<ID,VALUE> executionContext = ((PassageExecutionContext<ID, VALUE>) context)
                    .clone()
                    .setQuery(subOp);

            PassagePullExecutor<ID,VALUE> executor = new PassagePullExecutor<>(executionContext);
            Iterator<BackendBindings<ID,VALUE>> iterator = executor.visit(subOp, Iter.of(new BackendBindings<>()));

            while (iterator.hasNext()) {
                produced.offer(iterator.next());
            }

            // let `PauseException` be thrown
        }, executorService);

        worker.whenComplete((result, failure) -> workers.remove(worker));

        return worker;
    }
}
