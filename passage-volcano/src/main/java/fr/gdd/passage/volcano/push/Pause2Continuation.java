package fr.gdd.passage.volcano.push;

import fr.gdd.passage.volcano.transforms.DeduplicateFilters;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.function.BinaryOperator;

/**
 * This class used to be a visitor, but now, everything is done within
 * the physical operators themselves. Therefore, this became a utility
 * class.
 */
public class Pause2Continuation {

    public static final Op DONE = OpTable.empty(); // actually produces an empty `VALUES` clause
    public static boolean isDone(Op op) { return op instanceof OpTable table && table.getTable().isEmpty(); }
    
    public static Op removeDuplicatedFilters(Op op) { return Transformer.transform(new DeduplicateFilters(), op); }

    public static final BinaryOperator<Op> removeEmptyOfUnion = (l, r) -> {
        if (isDone(l) && isDone(r)) return OpTable.empty();
        if (isDone(l)) return r;
        if (isDone(r)) return l;
        return OpUnion.create(l, r);
    };

    public static boolean notExecuted(Op before, Op after) {
        return before.equalTo(after, null);
    }

}
