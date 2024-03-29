package fr.gdd.sage.sager.pause;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.sager.BindingId2Value;
import fr.gdd.sage.sager.HashMapWithPtrs;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.iterators.SagerScan;
import fr.gdd.sage.sager.iterators.SagerUnion;
import fr.gdd.sage.sager.resume.Subqueries2LeftOfJoins;
import org.apache.http.client.utils.CloneUtils;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Generate a SPARQL query from the paused state.
 */
public class Save2SPARQL extends ReturningOpVisitor<Op> {

    final JenaBackend backend;
    final Op root; // origin
    Op saved; // preempted
    Op caller;
    final HashMapWithPtrs<Op, Iterator<BindingId2Value>> op2it = new HashMapWithPtrs<>();

    public Save2SPARQL(Op root, ExecutionContext context) {
        this.root = root;
        this.backend = context.getContext().get(SagerConstants.BACKEND);
    }

    public void register(Op op, Iterator<BindingId2Value> it) {op2it.put(op, it);}
    public void unregister(Op op) {op2it.remove(op);}

    /* **************************************************************************** */

    public Op save(Op caller) {
        this.caller = caller;
        this.saved = ReturningOpVisitorRouter.visit(this, root);
        this.saved = ReturningOpVisitorRouter.visit(new Triples2BGP(), this.saved);
        this.saved = ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), this.saved);
        return this.saved;
    }

    @Override
    public Op visit(OpTriple triple) {
        SagerScan it = (SagerScan) op2it.get(triple);

        if (Objects.isNull(it)) {return null;}

        // OpTriple must remain the same, we cannot transform it by setting
        // the variables that are bound since the join variable would not match
        // after...
        return new OpSlice(it.asOpTriple(), it.offset(), Long.MIN_VALUE);
    }

    @Override
    public Op visit(OpJoin join) {
        FullyPreempted fp = new FullyPreempted(this);
        Op leftFullyPreempt = ReturningOpVisitorRouter.visit(fp, join.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, join.getRight());

        // TODO left + right only if left is preemptable
        boolean shouldI = ReturningOpVisitorRouter.visit(new ShouldPreempt(this), join.getLeft());
        if (shouldI) {
            Op left = ReturningOpVisitorRouter.visit(this, join.getLeft());
            return OpUnion.create(
                    distributeJoin(leftFullyPreempt, right), // preempted
                    OpJoin.create(left, join.getRight()) // rest
            );
        } else {
            return distributeJoin(leftFullyPreempt, right);
        }
    }

    @Override
    public Op visit(OpUnion union) {
        SagerUnion u = (SagerUnion) op2it.get(union);
        if (Objects.isNull(u)) {
            return union;
        }

        if (u.onLeft()) {
            Op left = ReturningOpVisitorRouter.visit(this, union.getLeft());
            return OpUnion.create(left, union.getRight());
        } else { // on right
            return  ReturningOpVisitorRouter.visit(this, union.getRight());
        }
    }

    @Override
    public Op visit(OpSlice slice) {
        if (slice.getSubOp() instanceof OpTriple triple) {
            return ReturningOpVisitorRouter.visit(this, triple);
        }
        throw new UnsupportedOperationException("TODO OpSlice cannot be saved right now."); // TODO
    }


    @Override
    public Op visit(OpExtend extend) {
        return OpCloningUtil.clone(extend, ReturningOpVisitorRouter.visit(this, extend.getSubOp()));
    }

    /* ************************************************************ */

    public static Op distributeJoin(Op op, Op over) {
        List<Op> ops = SagerOpExecutor.flattenUnion(over);
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

    public static Op unionize(List<Op> ops) {
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

    public HashMapWithPtrs<Op, Iterator<BindingId2Value>> getOp2it() {
        return op2it;
    }
}
