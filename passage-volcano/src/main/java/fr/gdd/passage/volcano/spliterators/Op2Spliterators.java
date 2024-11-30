package fr.gdd.passage.volcano.spliterators;

import org.apache.jena.sparql.algebra.Op;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Op2Spliterators<ID,VALUE> {

    final ConcurrentHashMap<Op, ConcurrentHashSet<PausableSpliterator<ID,VALUE>>> op2its = new ConcurrentHashMap<>();

    public void register(Op op, PausableSpliterator<ID,VALUE> it) {
        this.op2its.putIfAbsent(op, new ConcurrentHashSet<>());
        this.op2its.get(op).add(it);
    }

    public void unregister(Op op, PausableSpliterator<ID,VALUE> it) {
        this.op2its.get(op).remove(it);
    }

    public Set<PausableSpliterator<ID,VALUE>> get(Op op) {
        return this.op2its.get(op);
    }

}
