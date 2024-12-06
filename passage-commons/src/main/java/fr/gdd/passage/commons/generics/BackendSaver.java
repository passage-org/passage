package fr.gdd.passage.commons.generics;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.sparql.algebra.Op;

import java.io.Serializable;
import java.util.IdentityHashMap;
import java.util.Iterator;

/**
 * Save the state of a query execution based on its plan and
 * associated iterators registered during execution. The state is
 * saved as a tree of SPARQL operators, i.e., it's a logical plan
 * as well.
 * This class in itself does not live, a class must extend and implement
 * the operators of `ReturningOpVisitor<Op>`, otherwise it will throw
 * at runtime.
 */
public class BackendSaver<ID,VALUE,OFFSET extends Serializable> extends ReturningOpVisitor<Op> {

    final Backend<ID,VALUE> backend;
    final Op root; // origin

    // most important: during execution, the iterators that matter are saved within this structure.
    final IdentityHashMap<Op, Iterator<BackendBindings<ID, VALUE>>> op2it = new IdentityHashMap<>();

    public BackendSaver(Backend<ID,VALUE> backend, Op root) {
        this.backend = backend;
        this.root = root;
    }

    public BackendSaver<ID,VALUE,OFFSET> register(Op op, Iterator<BackendBindings<ID, VALUE>> it) {
        op2it.put(op, it);
        return this;
    }

    public BackendSaver<ID,VALUE,OFFSET> unregister(Op op) {
        op2it.remove(op);
        return this;
    }

    public Op getRoot() {return root;}
    public Iterator<BackendBindings<ID, VALUE>> getIterator(Op op) {return op2it.get(op);}

    /**
     * @return A SPARQL query that represents the state of the engine.
     */
    public Op save() {throw new UnsupportedOperationException("save");};
}
