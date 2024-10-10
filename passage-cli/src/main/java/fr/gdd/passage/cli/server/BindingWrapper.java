package fr.gdd.passage.cli.server;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BindingWrapper implements QueryIterator {

    final Iterator<BackendBindings> wrapped;
    final PassageOpExecutor executor;
    Thread producer;
    BlockingQueue<Binding> buffer = new LinkedBlockingQueue<>(2_000_000);
    Binding current;

    public BindingWrapper(Iterator<BackendBindings> wrapped, PassageOpExecutor executor) {
        this.wrapped = wrapped;
        this.executor = executor;

        producer = new Thread(){
            public void run(){
                while (wrapped.hasNext()) {
                    try {
                        BackendBindings next = wrapped.next();

                        BindingBuilder builder = BindingFactory.builder();
                        Set<Var> vars = next.vars();
                        for (Var v : vars) {
                            builder.add(v, NodeValueNode.parse(next.get(v).getString()).getNode());
                        }
                        buffer.put(builder.build());
                    } catch (InterruptedException e) {
                        // TODO double check that it does not interrupt when waiting for queue to get consumed
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        producer.start(); // we start immediately to get results
    }

    @Override
    public Binding next() {
        return current;
    }

    @Override
    public Binding nextBinding() {
        return this.next();
    }

    @Override
    public void cancel() {
        executor.pauseAsString();
    }

    @Override
    public boolean isJoinIdentity() {
        throw new UnsupportedOperationException("is join identity not implemented…");
    }

    @Override
    public boolean hasNext() {
        try {
            current = buffer.take();
            return true;
        } catch (InterruptedException e) {
            return false;
            // throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        executor.pauseAsString();
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
