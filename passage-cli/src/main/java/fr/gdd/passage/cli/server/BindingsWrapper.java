package fr.gdd.passage.cli.server;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;

import java.util.Iterator;

public class BindingsWrapper implements QueryIterator {

    final Iterator<BackendBindings> wrapped;
    final PassageOpExecutor executor;

    public BindingsWrapper(Iterator<BackendBindings> wrapped, PassageOpExecutor executor) {
        this.wrapped = wrapped;
        this.executor = executor;
    }

    @Override
    public Binding next() {
        return new BindingWrapper(wrapped.next());
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
        return this.wrapped.hasNext();
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
