package fr.gdd.jena.utils;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.ArrayList;
import java.util.List;

public class FlattenUnflatten {

    /**
     * @param op The operator to visit.
     * @return The list of operators that are under the (nested) unions.
     */
    public static List<Op> flattenUnion(Op op) {
        return switch (op) {
            case OpUnion u -> {
                List<Op> ops = new ArrayList<>();
                ops.addAll(flattenUnion(u.getLeft()));
                ops.addAll(flattenUnion(u.getRight()));
                yield ops;
            }
            case null -> List.of();
            default -> List.of(op);
        };
    }

    /**
     * @param op The operator to visit.
     * @return The list of operators that are under the (nested) joins.
     */
    public static List<Op> flattenJoin(Op op) {
        return switch (op) {
            case OpJoin j -> {
                List<Op> ops = new ArrayList<>();
                ops.addAll(flattenJoin(j.getLeft()));
                ops.addAll(flattenJoin(j.getRight()));
                yield ops;
            }
            case OpExtend e -> {
                List<Op> ops = new ArrayList<>();
                ops.add(OpCloningUtil.clone(e, OpTable.unit()));
                ops.addAll(flattenJoin(e.getSubOp()));
                yield ops;
            }
            case null -> List.of();
            default -> List.of(op);
        };
    }

}
