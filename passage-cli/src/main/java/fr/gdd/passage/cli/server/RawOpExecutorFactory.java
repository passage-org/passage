package fr.gdd.passage.cli.server;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.raw.executor.RawConstants;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.query.Query;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.Symbol;

import java.util.Iterator;
import java.util.Set;

public class RawOpExecutorFactory implements OpExecutorFactory {

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new OpExecutorWrapper(execCxt);
    }

    private static final Symbol userTimeoutSymbol = Symbol.create("timeout");
    private static final Symbol userAttemptsSymbol = Symbol.create("attempts");

    public static class OpExecutorWrapper extends OpExecutor {

        final RawOpExecutor rawer;

        public OpExecutorWrapper(ExecutionContext ec) {
            super(ec);


            if (ec.getContext().isDefined(userTimeoutSymbol)) {
                long userTimeout;
                try {
                    userTimeout = Long.parseLong(ec.getContext().get(userTimeoutSymbol));
                } catch (NumberFormatException | NullPointerException e) {
                    userTimeout = Long.MAX_VALUE;
                }
                ec.getContext().remove(userTimeoutSymbol); // Cleaning up the context
                long serverTimeout = ec.getContext().getLong(RawConstants.TIMEOUT, Long.MAX_VALUE);
                ec.getContext().set(RawConstants.TIMEOUT, Math.min(serverTimeout, userTimeout));
            }

            if (ec.getContext().isDefined(userAttemptsSymbol)) {
                long userAttempts;
                try {
                    userAttempts = Long.parseLong(ec.getContext().get(userAttemptsSymbol));
                } catch (NumberFormatException | NullPointerException e) {
                    userAttempts = Long.MAX_VALUE;
                }
                ec.getContext().remove(userAttemptsSymbol); // Cleaning up the context
                long serverAttempts = ec.getContext().getLong(RawConstants.ATTEMPT_LIMIT, Long.MAX_VALUE);
                ec.getContext().set(RawConstants.ATTEMPT_LIMIT, Math.min(serverAttempts, userAttempts));
            }

            Query q = ec.getContext().get(ARQConstants.sysCurrentQuery);
            rawer = new RawOpExecutor(ec);
            rawer.setBackend(ec.getContext().get(BackendConstants.BACKEND));
            rawer.setLimit(ec.getContext().get(RawConstants.LIMIT));
            rawer.setTimeout(ec.getContext().get(RawConstants.TIMEOUT));
        }

        @Override
        public QueryIterator executeOp(Op op, QueryIterator input) {
            return new BindingWrapper(rawer.execute(op), rawer);
        }

        @Override
        protected QueryIterator exec(Op op, QueryIterator input) {
            // for whatever reason, two things with same signature.
            // This one is actually called… not the public one.
            return this.executeOp(op, input);
        }

    }

    public static class BindingWrapper implements QueryIterator {

        final Iterator<BackendBindings> wrapped;
        final RawOpExecutor executor;

        public BindingWrapper(Iterator<BackendBindings> wrapped, RawOpExecutor executor) {
            this.wrapped = wrapped;
            this.executor = executor;
        }

        @Override
        public Binding next() {
            BackendBindings next = wrapped.next();
            BindingBuilder builder = BindingFactory.builder();
            Set<Var> vars = next.variables();
            for (Var v : vars) {
                //builder.add(v, NodeValueNode.parse(next.getBinding(v).getString()).getNode());
                builder.add(v, next.get(v));
            }
            return builder.build();
        }

        @Override
        public Binding nextBinding() {
            return this.next();
        }

        @Override
        public void cancel() {
            // TODO
        }

        @Override
        public boolean isJoinIdentity() {
            throw new UnsupportedOperationException("is join identity not implemented…");
        }

        @Override
        public boolean hasNext() {
            return this.wrapped.hasNext();
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

        @Override
        public void close() {
            // TODO
        }
    }
}
