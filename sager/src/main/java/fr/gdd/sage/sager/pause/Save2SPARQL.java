package fr.gdd.sage.sager.pause;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.PtrMap;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.iterators.SagerScan;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import fr.gdd.sage.sager.resume.Subqueries2LeftOfJoins;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Generate a SPARQL query from the paused state.
 */
public class Save2SPARQL<ID, VALUE> extends ReturningOpVisitor<Op> {

    final Backend<ID, VALUE, Long> backend;
    final Op root; // origin

    Op caller;
    public final PtrMap<Op, Iterator<BackendBindings<ID, VALUE>>> op2it = new PtrMap<>();

    public Save2SPARQL(Op root, ExecutionContext context) {
        this.root = root;
        this.backend = context.getContext().get(SagerConstants.BACKEND);
    }

    public void register(Op op, Iterator<BackendBindings<ID, VALUE>> it) {op2it.put(op, it);}
    public void unregister(Op op) {op2it.remove(op);}

    /* **************************************************************************** */

    public Op save(Op caller) {
        this.caller = caller;
        Op saved = ReturningOpVisitorRouter.visit(this, root);
        saved = ReturningOpVisitorRouter.visit(new Triples2BGP(), saved);
        saved = ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), saved);
        return saved;
    }

    @Override
    public Op visit(OpTriple triple) {
        SagerScanFactory<ID, VALUE> it = (SagerScanFactory<ID, VALUE>) op2it.get(triple);

        if (Objects.isNull(it)) {return null;}

        // OpTriple must remain the same, we cannot transform it by setting
        // the variables that are bound since the join variable would not match
        // after...
        return it.preempt();
        // return new OpSlice(triple, it.current(), Long.MIN_VALUE);
    }

    @Override
    public Op visit(OpJoin join) {
        Op right = ReturningOpVisitorRouter.visit(this, join.getRight());
        Op left = ReturningOpVisitorRouter.visit(this, join.getLeft());

        return FlattenUnflatten.unflattenUnion(List.of(right, OpJoin.create(left, join.getRight())));

//        FullyPreempted<ID,VALUE> fp = new FullyPreempted<>(this);
//        Op leftFullyPreempt = ReturningOpVisitorRouter.visit(fp, join.getLeft());
//        Op right = ReturningOpVisitorRouter.visit(this, join.getRight());
//
//        // TODO left + right only if left is preemptable
//        boolean shouldI = ReturningOpVisitorRouter.visit(new ShouldPreempt(this), join.getLeft());
//        if (shouldI) {
//            Op left = ReturningOpVisitorRouter.visit(this, join.getLeft());
//            return OpUnion.create(
//                    distributeJoin(leftFullyPreempt, right), // preempted
//                    OpJoin.create(left, join.getRight()) // rest
//            );
//        } else {
//            return distributeJoin(leftFullyPreempt, right);
//        }
        // throw new UnsupportedOperationException("join");
    }
//
//    @Override
//    public Op visit(OpUnion union) {
//        SagerUnion u = (SagerUnion) op2it.get(union);
//        if (Objects.isNull(u)) {
//            return union;
//        }
//
//        if (u.onLeft()) {
//            Op left = ReturningOpVisitorRouter.visit(this, union.getLeft());
//            return OpUnion.create(left, union.getRight());
//        } else { // on right
//            return  ReturningOpVisitorRouter.visit(this, union.getRight());
//        }
//    }

    @Override
    public Op visit(OpSlice slice) {
        if (slice.getSubOp() instanceof OpTriple triple) {
            // behaves as if it does not exist since the tp is interpreted as tp with skip.
            // If need be, the tp will add the slice OFFSET itself.
            return ReturningOpVisitorRouter.visit(this, triple);
        }
        throw new UnsupportedOperationException("TODO OpSlice cannot be saved right now."); // TODO
    }


//    @Override
//    public Op visit(OpExtend extend) {
//        return OpCloningUtil.clone(extend, ReturningOpVisitorRouter.visit(this, extend.getSubOp()));
//    }

    /* ************************************************************ */

    public static Op distributeJoin(Op op, Op over) {
        List<Op> ops = FlattenUnflatten.flattenUnion(over);
        return switch (ops.size()) {
            case 0 -> op;
            case 1 -> OpJoin.create(op, over);
            default -> {
                Op left = ops.get(0);
                for (int i = 1; i < ops.size(); ++i) {
                    Op right = OpJoin.create(op, ops.get(i));
                    left = OpUnion.create(left, right);
                }
                yield left;
            }
        };
    }

}
