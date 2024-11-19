package fr.gdd.raw.cli.server;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.raw.executor.RawConstants;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.Iterator;
import java.util.Set;

public class RawOpExecutorFactory implements OpExecutorFactory {

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new OpExecutorWrapper(execCxt);
    }

    public static class OpExecutorWrapper extends OpExecutor {

        final RawOpExecutor rawer;

        public OpExecutorWrapper(ExecutionContext ec) {
            super(ec);
            rawer = new RawOpExecutor(ec);
            rawer.setBackend(ec.getContext().get(BackendConstants.BACKEND));
            rawer.setLimit(ec.getContext().get(RawConstants.LIMIT));
//            rawer.setTimeout(ec.getContext().get(RawConstants.TIMEOUT));
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
                builder.add(v, NodeValueNode.parse(next.getBinding(v).getString()).getNode());
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
