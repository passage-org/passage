package fr.gdd.passage.cli.server;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.transforms.DefaultGraphURIQueryWrapper;
import fr.gdd.passage.volcano.PassageConstants;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.query.ARQ;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;

import java.util.Iterator;
import java.util.Set;

/**
 * Regular Blazegraph engine to expose to the `sparql`. Blazegraph may consume a lot
 * of resources (threads + memory). Long query should be deleted properly to avoid consuming
 * forever the resources.
 */
public class BlazegraphOpExecutorFactory implements OpExecutorFactory {

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return new BlazegraphOpExecutor(execCxt);
    }

    public static class BlazegraphOpExecutor extends OpExecutor {

        final BlazegraphBackend backend;
        final Long timeoutMillis;

        protected BlazegraphOpExecutor(ExecutionContext ec) {
            super(ec);
            this.backend = ec.getContext().get(BackendConstants.BACKEND);
            this.timeoutMillis = ec.getContext().get(PassageConstants.TIMEOUT); // null is infinity
        }

        @Override
        public QueryIterator executeOp(Op op, QueryIterator input) {
            return null;
        }

        @Override
        protected QueryIterator exec(Op op, QueryIterator input) {
            try {
                Op opOnGraph = new DefaultGraphURIQueryWrapper(execCxt).visit(op);
                Iterator<BindingSet> ibs = backend.executeQueryToIterator(OpAsQuery.asQuery(opOnGraph).toString(),
                        this.timeoutMillis);
                return new BindingWrapper(ibs, backend);
            } catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class BindingWrapper implements QueryIterator {

        final Iterator<BindingSet> wrapped;
        final Backend backend;

        public BindingWrapper(Iterator<BindingSet> wrapped, Backend backend) {
            this.wrapped = wrapped;
            this.backend = backend;
        }

        @Override
        public Binding next() {
            if(!wrapped.hasNext()) return null;
            BindingSet next = wrapped.next();

            BindingBuilder builder = BindingFactory.builder();
            Set<String> varStrings = next.getBindingNames();;
            for (String v : varStrings) {
                // Is this the best way from openrdf binding to jena binding?
                builder.add(Var.alloc(v),
                        NodeValue.toNode(
                                NodeValueNode.parse(
                                        stringify(next.getBinding(v).getValue())
                                )
                        )
                );
            }
            return builder.build();
        }

        @Override
        public Binding nextBinding() {
            return this.next();
        }

        public String stringify(Value value) {
            return switch (value){
                case BigdataURI bigdataURI:
                    yield "<" + bigdataURI.stringValue() + ">";
                case BigdataLiteral bigdataLiteral:
                    yield "\"" + bigdataLiteral.stringValue() + "\"";
                case BigdataBNode bigdataBNode:
                    {} // What to do here?
                default:
                    throw new IllegalStateException("Unexpected value: " + value);
            };
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
            return wrapped.hasNext();
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
