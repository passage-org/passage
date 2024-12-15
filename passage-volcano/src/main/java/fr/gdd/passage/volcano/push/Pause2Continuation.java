package fr.gdd.passage.volcano.push;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.function.BinaryOperator;

public class Pause2Continuation<ID,VALUE> {

    public static final Op DONE = OpTable.empty();
    public static boolean isDone(Op op) { return op instanceof OpTable table && table.getTable().isEmpty(); }

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
