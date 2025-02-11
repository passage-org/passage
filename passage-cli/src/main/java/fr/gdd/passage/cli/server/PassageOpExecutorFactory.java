package fr.gdd.passage.cli.server;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Apache Jena likes OpExecutor which is not an interface but a concrete
 * implementation. So we wrap this to keep ours clean.
 */
public class PassageOpExecutorFactory implements OpExecutorFactory {

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new OpExecutorWrapper(execCxt);
    }

    /**
     * The actual wrapper of OpExecutor for Passage. It builds its execution context based on
     * the dataset context.
     */
    public static class OpExecutorWrapper extends OpExecutor {

        final PassagePushExecutor<?,?> executor;

        public OpExecutorWrapper(ExecutionContext ec) {
            super(ec);
            executor = new PassagePushExecutor<>(new PassageExecutionContextBuilder<>()
                    .setTimeout(ec.getContext().get(PassageConstants.TIMEOUT))
                    .setBackend(ec.getContext().get(BackendConstants.BACKEND))
                    .setMaxScans(ec.getContext().get(PassageConstants.MAX_SCANS))
                    .setMaxParallel(ec.getContext().get(PassageConstants.MAX_PARALLELISM))
                    .setExecutorFactory((_ec) -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) _ec))
                    .setContext(ec)
                    .build());
        }

        @Override
        public QueryIterator executeOp(Op op, QueryIterator input) {
            return new BindingWrapper(op, executor);
        }

        @Override
        protected QueryIterator exec(Op op, QueryIterator input) {
            // for whatever reason, two things with same signature.
            // This one is actually called… not the public one.
            return this.executeOp(op, input);
        }

    }

    /**
     * The wrapper for the stream that might be parallel.
     */
    public static class BindingWrapper implements QueryIterator {

        final BlockingQueue<BackendBindings<?,?>> buffer;
        final Op query;
        final PassagePushExecutor<?,?> executor;
        final Op paused;

        public BindingWrapper(Op query, PassagePushExecutor<?,?> executor) {
            this.query = query;
            this.executor = executor;
            this.buffer = new ArrayBlockingQueue<>(100_000_000); // TODO put this as an argument
            this.paused = this.executor.execute(query, binding -> {
                try {
                    buffer.put(binding);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public boolean hasNext() {
            while (this.buffer.isEmpty() && !this.executor.isDone()) {
                // just wait // TODO TODO use synchro and awaits
            }

            return !this.buffer.isEmpty();
        }

        @Override
        public Binding next() {
            return this.buffer.poll();
        }

        @Override
        public Binding nextBinding() {
            return this.next();
        }

        @Override
        public void cancel() {
            // TODO TODO TODO
            // executor.pauseAsString();
        }

        @Override
        public boolean isJoinIdentity() {
            throw new UnsupportedOperationException("is join identity not implemented…");
        }



        @Override
        public void close() {
            // TODO TODO TODO
            // executor.pauseAsString();
        }

        @Override
        public void output(IndentedWriter out, SerializationContext sCxt) {
            throw new UnsupportedOperationException("output for iterator not implemented…");
        }

        @Override
        public String toString(PrefixMapping pmap) {
            throw new UnsupportedOperationException("toString pmap not implemented…");
        }

        @Override
        public void output(IndentedWriter out) {
            throw new UnsupportedOperationException("output for iterator not implemented…");
        }
    }
}
