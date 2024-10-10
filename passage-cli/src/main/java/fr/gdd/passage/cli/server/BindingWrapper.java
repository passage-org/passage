package fr.gdd.passage.cli.server;

import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;

import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;

public class BindingWrapper implements Binding {

    final BackendBindings<?,?> wrapped;
    final Iterator<Binding> iterator;

    public BindingWrapper(BackendBindings<?,?> wrapped) {
        this.wrapped = wrapped;
        this.iterator = wrapped.vars().stream().map(v -> {
            BindingBuilder builder = BindingFactory.builder();
            builder.add(v, toNode(v));
            return builder.build();
        }).iterator();
    }

    public BackendBindings<?, ?> getWrapped() {
        return wrapped;
    }

    @Override
    public Iterator<Var> vars() {
        return wrapped.vars().iterator();
    }

    @Override
    public Set<Var> varsMentioned() {
        return wrapped.vars();
    }

    @Override
    public void forEach(BiConsumer<Var, Node> action) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public boolean contains(Var var) {
        return this.wrapped.contains(var);
    }

    @Override
    public Node get(Var var) {
        return toNode(var);
    }

    @Override
    public int size() {
        return this.wrapped.size();
    }

    @Override
    public boolean isEmpty() {
        return this.wrapped.size() == 0;
    }

    private Node toNode (Var v) {
        return NodeValueNode.parse(wrapped.get(v).getString()).getNode();
    }
}
