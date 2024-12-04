package fr.gdd.passage.volcano.push;

import com.google.common.collect.ConcurrentHashMultiset;
import fr.gdd.passage.commons.generics.ConcurrentPtrMap;
import fr.gdd.passage.commons.generics.IPtrMap;
import fr.gdd.passage.commons.generics.PtrMap;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.jena.sparql.algebra.Op;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.*;

/**
 * Register the actual physical operators for a logical operator. Since
 * the processing can be parallel, there may be multiple physical operators
 * for a single operator.
 */
public class Op2Spliterators<ID,VALUE> {

    // ugly, we keep two data structures dedicated to keep track of physical operators
    final ConcurrentPtrMap<Op, ConcurrentHashMultiset<PausableSpliterator<ID,VALUE>>> op2its;
    final PtrMap<Op, PausableSpliterator<ID,VALUE>> op2it;
    final boolean isParallel;

    public Op2Spliterators(boolean isParallel) {
        this.op2its = new ConcurrentPtrMap<>();
        this.op2it = new PtrMap<>();
        this.isParallel = isParallel;
    }

    public void register(Op op, PausableSpliterator<ID,VALUE> it) {
        if (!isParallel) {
            this.op2it.put(op, it);
        } else {
            ConcurrentHashMultiset<PausableSpliterator<ID,VALUE>> chm = ConcurrentHashMultiset.create();
            chm = this.op2its.putIfAbsent(op, chm);
            chm.add(it);
        }
    }

    public void unregister(Op op, PausableSpliterator<ID,VALUE> it) {
        if (!isParallel) {
            this.op2it.remove(op);
        } else {
            this.op2its.get(op).remove(it);
        }
    }

    public Set<PausableSpliterator<ID,VALUE>> get(Op op) {
        if (isParallel) {
            return this.op2its.get(op).elementSet();
        } else {
            // `Set.of` but should not be often
            return Objects.nonNull(this.op2it.get(op)) ? Set.of(this.op2it.get(op)): Set.of();
        }
    }
}
