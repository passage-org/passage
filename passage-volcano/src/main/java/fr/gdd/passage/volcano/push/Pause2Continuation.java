package fr.gdd.passage.volcano.push;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * Generate a SPARQL query from the current paused state. By executing
 * the generated SPARQL query, the query execution becomes complete.
 */
public class Pause2Continuation<ID,VALUE> extends ReturningOpVisitor<Op> {

    final Op2Spliterators<ID,VALUE> op2its;

    public Pause2Continuation(Op2Spliterators<ID,VALUE> op2its) {
        this.op2its = op2its;
    }

    /**
     * @param root The root operator.
     * @return The continuation query of the original query, null if the query execution is over.
     */
    public Op get(Op root) {
        Op continuation = super.visit(root);
        // we don't override visit(Op) because null has a different meaning
        return isDone(continuation) ? null : continuation;
    }

    @Override
    public Op visit(OpTriple triple) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(triple);
        if (notExecuted(its)) return triple; // not executed at all
        if (isDone(its)) return DONE;
        return its.stream().map(PausableSpliterator::pause).reduce(OpTable.empty(), removeEmptyOfUnion);
    }

    @Override
    public Op visit(OpQuad quad) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(quad);
        if (notExecuted(its)) return quad; // not executed at all
        if (isDone(its)) return DONE;
        return its.stream().map(PausableSpliterator::pause).reduce(OpTable.empty(), removeEmptyOfUnion);
    }

    @Override
    public Op visit(OpJoin join) {
        // no state needed really, everything is in the returned value of these:
        Op left = super.visit(join.getLeft());
        Op right = super.visit(join.getRight());

        // not executed at all, we return it pristine
        if (notExecuted(left, join.getLeft()) && notExecuted(right, join.getRight())) return join;

        if (isDone(left) && isDone(right)) return DONE; // both done, so we are done
        if (isDone(left) && notExecuted(right, join.getRight())) return DONE; // consumed left but right did not move, so the join is done
        // all consumed left or kept still, but generating a new right to consume
        if (isDone(left) || notExecuted(left, join.getLeft())) return right;
        // still must do the rest of left with the old right
        if (isDone(right) || notExecuted(right, join.getRight())) return OpJoin.create(left, join.getRight());

        // Otherwise, create a union with:
        // (i) The preempted right part (The current pointer to where we are executing)
        // (ii) The preempted left part with a copy of the right (The rest of the query)
        // In other words, it's like, (i) finish the OFFSET you where in. (ii) start at OFFSET + 1
        return OpUnion.create(right, OpJoin.create(left, join.getRight()));
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(lj);
        if (Pause2Continuation.notExecuted(its)) { return lj; }
        if (isDone(its)) { return DONE; }
        return its.stream().map(PausableSpliterator::pause).reduce(DONE, removeEmptyOfUnion);
    }

    @Override
    public Op visit(OpUnion union) {
        Op left = super.visit(union.getLeft());
        Op right = super.visit(union.getRight());

        if (notExecuted(left, union.getLeft()) && notExecuted(right, union.getRight())) return union;

        if (isDone(left) && isDone(right)) return DONE;
        if (isDone(left)) return right;
        if (isDone(right)) return left;

        return OpUnion.create(left, right);
    }

    @Override
    public Op visit(OpSlice slice) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(slice);
        if (notExecuted(its)) return slice;
        if (isDone(its)) return DONE;
        return its.stream().map(PausableSpliterator::pause).reduce(DONE, removeEmptyOfUnion);
    }

    @Override
    public Op visit(OpTable table) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(table);
        if (notExecuted(its)) return table;
        if (isDone(its)) return DONE;
        return its.stream().map(PausableSpliterator::pause).reduce(DONE, removeEmptyOfUnion);
    }

    /* **************** UNARY OPERATORS WITHOUT INTERNAL STATES ******************* */

    @Override
    public Op visit(OpExtend extend) {
        Op subop = super.visit(extend.getSubOp());
        if (notExecuted(extend.getSubOp(), subop)) return extend;
        if (isDone(subop)) return DONE;
        return OpCloningUtil.clone(extend, subop);
    }

    @Override
    public Op visit(OpProject project) {
        Op subop = super.visit(project.getSubOp());
        if (notExecuted(project.getSubOp(), subop)) return project;
        if (isDone(subop)) return DONE;
        return OpCloningUtil.clone(project, subop);
    }

    @Override
    public Op visit(OpFilter filter) {
        Op subop = super.visit(filter.getSubOp());
        if (notExecuted(filter.getSubOp(), subop)) return filter;
        if (isDone(subop)) return DONE;
        return OpCloningUtil.clone(filter, subop);
    }


    /* *************************** UTILS ************************ */

    public static final Op DONE = OpTable.empty();
    public static boolean isDone(Op op) { return op instanceof OpTable table && table.getTable().isEmpty(); }
    public static <ID,VALUE> boolean isDone(Set<PausableSpliterator<ID,VALUE>> its) {return its.isEmpty(); }

    public static final BinaryOperator<Op> removeEmptyOfUnion = (l, r) -> {
        if (isDone(l) && isDone(r)) return OpTable.empty();
        if (isDone(l)) return r;
        if (isDone(r)) return l;
        return OpUnion.create(l, r);
    };

    public static boolean notExecuted(Op before, Op after) {
        return before.equalTo(after, null);
    }

    public static <ID,VALUE> boolean notExecuted(Set<PausableSpliterator<ID,VALUE>> its) {
        return Objects.isNull(its);
    }
}
