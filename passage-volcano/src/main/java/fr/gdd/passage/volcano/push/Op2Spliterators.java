package fr.gdd.passage.volcano.push;

import com.google.common.collect.ConcurrentHashMultiset;
import fr.gdd.passage.commons.generics.ConcurrentIdentityHashMap;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.jena.sparql.algebra.Op;

import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;

/**
 * Register the actual physical operators for a logical operator. Since
 * the processing can be parallel, there may be multiple physical operators
 * for a single operator.
 */
public class Op2Spliterators<ID,VALUE> {

    // ugly, we keep two data structures dedicated to keep track of physical operators
    final ConcurrentIdentityHashMap<Op, ConcurrentHashMultiset<PausableSpliterator<ID,VALUE>>> op2its;
    final IdentityHashMap<Op, PausableSpliterator<ID,VALUE>> op2it;
    final boolean isParallel;

    public Op2Spliterators(boolean isParallel) {
        this.op2its = new ConcurrentIdentityHashMap<>();
        this.op2it = new IdentityHashMap<>();
        this.isParallel = isParallel;
    }

    public void register(Op op, PausableSpliterator<ID,VALUE> it) {
        if (!isParallel) {
            this.op2it.put(op, it);
        } else {
            var wrapKey = ConcurrentIdentityHashMap.buildKeyOf(op);
            this.op2its.map.putIfAbsent(wrapKey, ConcurrentHashMultiset.create());
            this.op2its.map.computeIfPresent(wrapKey, (wk, its) -> { // atomic
                its.add(it);
                return its;
            });
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
            return Objects.nonNull(this.op2its.get(op)) ? this.op2its.get(op).elementSet():
                    null;
        } else {
            // `Set.of` but should not be often
            return Objects.nonNull(this.op2it.get(op)) ? Set.of(this.op2it.get(op)): Set.of();
        }
    }
}
