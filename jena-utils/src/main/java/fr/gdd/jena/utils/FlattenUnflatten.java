package fr.gdd.jena.utils;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * Multijoins and multiunions do not exist in SPARQL algebra, but they are
 * nice to manipulate, so we have a from/to conversion in case they are
 * needed.
 */
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
     * @param ops The list of operators to unionize.
     * @return A tree of operators linked by cascading unions.
     */
    public static Op unflattenUnion(List<Op> ops) {
        return switch (ops.size()) {
            case 0 -> null;
            case 1 -> ops.get(0);
            default -> {
                Op left = ops.get(0);
                for (int i = 1; i < ops.size(); ++i) {
                    Op right = ops.get(i);
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }

    /* ************************************************************************ */

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
