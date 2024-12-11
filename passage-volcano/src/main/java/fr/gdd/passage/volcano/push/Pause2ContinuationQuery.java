package fr.gdd.passage.volcano.push;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * Generate a SPARQL query from the current paused state. By executing
 * the generated SPARQL query, the query execution becomes complete.
 */
public class Pause2ContinuationQuery<ID,VALUE> extends ReturningOpVisitor<Op> {

    final Op2Spliterators<ID,VALUE> op2its;

    public Pause2ContinuationQuery(Op2Spliterators<ID,VALUE> op2its) {
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
        if (Objects.isNull(its)) return triple; // not executed at all
        if (its.isEmpty()) return DONE;
        return its.stream().map(PausableSpliterator::pause)
                .reduce(OpTable.empty(), removeEmptyOfUnion);
    }

    @Override
    public Op visit(OpQuad quad) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(quad);
        if (Objects.isNull(its)) return quad; // not executed at all
        if (its.isEmpty()) return DONE;
        return its.stream().map(PausableSpliterator::pause)
                .reduce(OpTable.empty(), removeEmptyOfUnion);
    }

    @Override
    public Op visit(OpJoin join) {
        // no state needed really, everything is in the returned value of these:
        Op left = super.visit(join.getLeft());
        Op right = super.visit(join.getRight());

        if (left == join.getLeft() && right == join.getRight()) return join; // not executed at all, we return it pristine

        if (isDone(left) && isDone(right)) return DONE; // both done, so we are done
        if (isDone(left) && right == join.getRight()) return DONE; // consumed left but right did not move, so the join is done
        if (isDone(left)) return right; // consumed left, generating a new right to consume
        if (isDone(right)) return OpJoin.create(left, join.getRight()); // still must do the rest of left with the old right

        if (left == join.getLeft()) return right;
        if (right == join.getRight()) return OpJoin.create(left, join.getRight());

        // if (right == join.getRight()) return OpJoin.create(left, right); // TODO bind gets consumed

        // Otherwise, create a union with:
        // (i) The preempted right part (The current pointer to where we are executing)
        // (ii) The preempted left part with a copy of the right (The rest of the query)
        // In other words, it's like, (i) finish the OFFSET you where in. (ii) start at OFFSET + 1
        return FlattenUnflatten.unflattenUnion(Arrays.asList(right, OpJoin.create(left, join.getRight())));
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        Set<PausableSpliterator<ID,VALUE>> optionals = op2its.get(lj);
        if (Objects.isNull(optionals) || optionals.isEmpty()) { return null; }

        return FlattenUnflatten.unflattenUnion(optionals.stream().map(PausableSpliterator::pause).collect(Collectors.toList()));
    }

    @Override
    public Op visit(OpUnion union) {
        Op left = super.visit(union.getLeft());
        Op right = super.visit(union.getRight());

        if (left == union.getLeft() && right == union.getRight()) return union;

        // left = Objects.isNull(left) ? union.getLeft() : left;
        // right = Objects.isNull(right) ? union.getRight() : right;

        if (isDone(left) && isDone(right)) return DONE;
        if (isDone(left)) return right;
        if (isDone(right)) return left;

        return FlattenUnflatten.unflattenUnion(Arrays.asList(left, right));
    }

    @Override
    public Op visit(OpSlice slice) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(slice);
        if (Objects.isNull(its)) return slice; // not executed at all, so we copy
        if (its.isEmpty()) return DONE; // done
        return its.stream().map(PausableSpliterator::pause).reduce(DONE, removeEmptyOfUnion);
    }

    @Override
    public Op visit(OpTable table) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(table);
        if (Objects.isNull(its)) return null; // not executed at all, so we copy
        if (its.isEmpty()) return OpTable.empty(); // done
        return its.stream().map(PausableSpliterator::pause)
                .reduce(OpTable.empty(), removeEmptyOfUnion);
    }

    /* **************** UNARY OPERATORS WITHOUT INTERNAL STATES ******************* */

    @Override
    public Op visit(OpExtend extend) {
        Op subop = super.visit(extend.getSubOp());
        if (Objects.isNull(subop)) return null; // downstream never executed
        if (isDone(subop)) return OpTable.empty(); // done propagates
        return OpCloningUtil.clone(extend, subop); // otherwise, we actually produce something
    }

    @Override
    public Op visit(OpProject project) {
        Op subop = super.visit(project.getSubOp());
        if (Objects.isNull(subop)) return null; // downstream never executed
        if (isDone(subop)) return OpTable.empty(); // done propagates
        return OpCloningUtil.clone(project, subop); // otherwise, we actually produce something
    }

    @Override
    public Op visit(OpFilter filter) {
        Op subop = super.visit(filter.getSubOp());
        if (Objects.isNull(subop)) return null; // downstream never executed
        if (isDone(subop)) return OpTable.empty(); // done propagates
        return OpCloningUtil.clone(filter, subop); // otherwise, we actually produce something
    }


    /* *************************** UTILS ************************ */

    public static Boolean isDone(Op op) { return op instanceof OpTable table && table.getTable().isEmpty(); }

    private static final BinaryOperator<Op> removeEmptyOfUnion = (l, r) -> {
        if (isDone(l) && isDone(r)) return OpTable.empty();
        if (isDone(l)) return r;
        if (isDone(r)) return l;
        return OpUnion.create(l, r);
    };

    public static final Op DONE = OpTable.empty();
}
