package fr.gdd.passage.volcano.push;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.passage.volcano.push.streams.PausableSpliterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Generate a SPARQL query from the current paused state. By executing
 * the generated SPARQL query, the query execution becomes complete.
 */
public class Pause2ContinuationQuery<ID,VALUE> extends ReturningOpVisitor<Op> {

    final Op2Spliterators<ID,VALUE> op2its;

    public Pause2ContinuationQuery(Op2Spliterators<ID,VALUE> op2its) {
        this.op2its = op2its;
    }

    @Override
    public Op visit(OpTriple triple) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(triple);
        if (Objects.isNull(its) || its.isEmpty()) return null;
        // unionize the lot of triple iterators
        return its.stream().map(PausableSpliterator::pause).reduce(null,
                (l, r) -> Objects.isNull(l) ? r : OpUnion.create(l, r));
    }

    @Override
    public Op visit(OpQuad quad) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(quad);
        if (Objects.isNull(its) || its.isEmpty()) return null;
        // unionize the lot of quads iterators
        return its.stream().map(PausableSpliterator::pause).reduce(null,
                (l, r) -> Objects.isNull(l) ? r : OpUnion.create(l, r));
    }

    @Override
    public Op visit(OpJoin join) {
        // no state needed really, everything is in the returned value of these:
        Op left = this.visit(join.getLeft());
        Op right = this.visit(join.getRight());

        // If the left is empty, i.e., it's done. Then you don't need to preempt it.
        // However, you still need to consider to preempt the right part.
        if (Objects.isNull(left)) {
            // (if right is null, it's handled by the union flattener)
            return FlattenUnflatten.unflattenUnion(Collections.singletonList(right));
        }

        // Otherwise, create a union with:
        // (i) The preempted right part (The current pointer to where we are executing)
        // (ii) The preempted left part with a copy of the right (The rest of the query)
        // In other words, it's like, (i) finish the OFFSET you where in. (ii) start at OFFSET + 1
        return FlattenUnflatten.unflattenUnion(Arrays.asList(right, OpJoin.create(left, join.getRight())));
    }

    @Override
    public Op visit(OpUnion union) {
        Op left = this.visit(union.getLeft());
        Op right = this.visit(union.getRight());

        if (Objects.isNull(left) && Objects.isNull(right)) {
            return null;
        } else if (Objects.isNull(left)) {
            return FlattenUnflatten.unflattenUnion(Collections.singletonList(right));
        } else if (Objects.isNull(right)) {
            return FlattenUnflatten.unflattenUnion(Collections.singletonList(left));
        }

        return FlattenUnflatten.unflattenUnion(Arrays.asList(left, right));
    }

    @Override
    public Op visit(OpSlice slice) {
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(slice);
        if (Objects.isNull(its) || its.isEmpty()) return null;
        return its.stream().map(PausableSpliterator::pause).reduce(null,
                (l, r) -> Objects.isNull(l) ? r : OpUnion.create(l, r));
    }

    @Override
    public Op visit(OpTable table) {
        if (table.isJoinIdentity()) {
            return null; // nothing to save
        }
        // otherwise values
        Set<PausableSpliterator<ID,VALUE>> its = op2its.get(table);
        if (Objects.isNull(its) || its.isEmpty()) return null;
        return its.stream().map(PausableSpliterator::pause).reduce(null,
                (l, r) -> Objects.isNull(l) ? r : OpUnion.create(l, r));
    }

    /* **************** UNARY OPERATORS WITHOUT INTERNAL STATES ******************* */

    @Override
    public Op visit(OpExtend extend) {
        Op subop = this.visit(extend.getSubOp());
        // TODO subop can be null because done, but should return done
        return Objects.isNull(subop) ? null : OpCloningUtil.clone(extend, subop);
    }

    @Override
    public Op visit(OpProject project) {
        Op subop = this.visit(project.getSubOp());
        return Objects.isNull(subop) ? null : OpCloningUtil.clone(project, subop);
    }

    @Override
    public Op visit(OpFilter filter) {
        Op subop = this.visit(filter.getSubOp());
        return Objects.isNull(subop) ? null : OpCloningUtil.clone(filter, subop);
    }
}
