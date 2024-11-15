package fr.gdd.passage.volcano.pause;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendSaver;
import fr.gdd.passage.volcano.iterators.aggregate.PassageCount;
import fr.gdd.passage.volcano.iterators.distinct.PassageDistinct;
import fr.gdd.passage.volcano.iterators.limitoffset.PassageLimitOffsetFactory;
import fr.gdd.passage.volcano.iterators.optional.PassageOptional;
import fr.gdd.passage.volcano.iterators.scan.PassageScan;
import fr.gdd.passage.volcano.iterators.union.PassageUnion;
import fr.gdd.passage.volcano.iterators.values.PassageValues;
import fr.gdd.passage.volcano.iterators.limitoffset.CanBeSkipped;
import fr.gdd.passage.volcano.transforms.Quad2Pattern;
import fr.gdd.passage.volcano.transforms.Subqueries2LeftOfJoins;
import fr.gdd.passage.volcano.transforms.Triples2BGP;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

/**
 * Generate a SPARQL query from the current paused state. By executing
 * the generated SPARQL query, the query execution becomes complete.
 */
public class Pause2Next<ID, VALUE> extends BackendSaver<ID,VALUE,Long> {

    public Pause2Next(ExecutionContext context) {
        super(context.getContext().get(BackendConstants.BACKEND),null);
    }
    public Pause2Next(Op root, ExecutionContext context) {
        super(context.getContext().get(BackendConstants.BACKEND), root);
    }

    @Override
    public Op save() {
        Op saved = ReturningOpVisitorRouter.visit(this, getRoot());
        saved = Objects.isNull(saved) ? null : new Triples2BGP().visit(saved);
        saved = Objects.isNull(saved) ? null : new Subqueries2LeftOfJoins().visit(saved); // TODO remove this
        saved = Objects.isNull(saved) ? null : new Quad2Pattern().visit(saved);
        return saved;
    }

    @Override
    public Op visit(OpProject project) {
        Op subop =  ReturningOpVisitorRouter.visit(this, project.getSubOp());
        return Objects.isNull(subop) ? null : OpCloningUtil.clone(project, subop);
    }

    @Override
    public Op visit(OpDistinct distinct) {
        PassageDistinct<ID, VALUE> it = (PassageDistinct<ID, VALUE>) getIterator(distinct);

        // If the scan iterator is not registered, it should not be preempted
        if (Objects.isNull(it)) {return null;}

        Op subop = ReturningOpVisitorRouter.visit(this, distinct.getSubOp());

        if (Objects.isNull(subop)) {return null;}

        if (subop instanceof OpFilter filter) {
            // TODO check that this cannot be a filter from somewhere else
            // TODO btw, it would not be wrong semantically-wise to stack them, however
            // TODO this would consume space.
            subop = filter.getSubOp(); // remove the last filter applied
        }

        subop = it.getFilter(subop);

        return OpCloningUtil.clone(distinct, subop);
    }

    @Override
    public Op visit(OpFilter filter) {
        Op subop = ReturningOpVisitorRouter.visit(this, filter.getSubOp());
        return Objects.isNull(subop) ? null : OpCloningUtil.clone(filter, subop);
    }

    @Override
    public Op visit(OpTriple triple) {
        // It gets a ScanFactory instead of a Scan because the factory
        // contains the input binding that constitutes the state of the scan.
        PassageScan<ID, VALUE> it = (PassageScan<ID, VALUE>) getIterator(triple);

        // If the scan iterator is not registered, it should not be preempted
        if (Objects.isNull(it)) {return null;}

        // In `it.preempt()`, it returns null if the scan does not have a next, i.e.,
        // if the can is done. This aims at removing the branches of the plans that are
        // done, finished. Otherwise, they would pollute the preempted plan forever.
        // It creates a subquery including bindings with `BIND AS`, then the triple
        // pattern. Then wraps it all with `OFFSET`.
        return it.pause();
    }

    @Override
    public Op visit(OpQuad quad) { // very identical to OpTriple
        PassageScan<ID, VALUE> it = (PassageScan<ID, VALUE>) getIterator(quad);
        if (Objects.isNull(it)) {return null;}
        return it.pause();
    }

    @Override
    public Op visit(OpJoin join) {
        // no state needed really, everything is in the returned value of these:
        Op left = ReturningOpVisitorRouter.visit(this, join.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, join.getRight());

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
        PassageUnion<ID,VALUE> u = (PassageUnion<ID,VALUE>) getIterator(union);

        // not executed yet, otherwise would exist. So no need to preempt it.
        if (Objects.isNull(u)) {return null;}

        return u.pause();
    }

    @Override
    public Op visit(OpSlice slice) {
        CanBeSkipped canBeSkipped = new CanBeSkipped();
        if (canBeSkipped.visit((Op) slice)) { // simple OFFSET LIMIT query with only one triple pattern inside
            PassageLimitOffsetFactory.CompatibilityCheckIterator it = (PassageLimitOffsetFactory.CompatibilityCheckIterator) getIterator(slice);
            if (Objects.isNull(it)) { return null; }
            Op tripleOrQuadPaused = ReturningOpVisitorRouter.visit(this, canBeSkipped.getTripleOrQuad());
            return it.pause(tripleOrQuadPaused);
        } else { // complex OFFSET LIMIT query or subquery
            PassageLimitOffsetFactory.CompatibilityCheckIterator it = (PassageLimitOffsetFactory.CompatibilityCheckIterator) getIterator(slice);
            if (Objects.isNull(it)) return null;
            return it.pause(super.visit(slice.getSubOp()));
        }
    }

    @Override
    public Op visit(OpExtend extend) { // cloned
        Op subop = ReturningOpVisitorRouter.visit(this, extend.getSubOp());

        if (Objects.nonNull(subop) && subop instanceof OpGroup groupBy) {
            // performed here because the value is saved in extend
            PassageCount<ID,VALUE> agg = (PassageCount<ID, VALUE>) getIterator(groupBy);

            Op subopGB = ReturningOpVisitorRouter.visit(this, groupBy.getSubOp());

            if (Objects.isNull(subopGB)) {return null;}

            return agg.save(extend, subopGB);
        }

        return Objects.isNull(subop) ? null : OpCloningUtil.clone(extend, subop);
    }

    @Override
    public Op visit(OpTable table) {
        if (table.isJoinIdentity()) {
            return null;
        }
        // preempt a VALUES: only copy the rest
        PassageValues<ID,VALUE> values = (PassageValues<ID, VALUE>) getIterator(table);
        if (Objects.isNull(values)) {
            return null;
        }
        return values.pause();
    }

    @Override
    public Op visit(OpConditional cond) {
        throw new UnsupportedOperationException("Copy the behavior of OPLeftJoin"); // TODO TODO TODO
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        if (Objects.nonNull(lj.getExprs()) && !lj.getExprs().isEmpty()) {
            throw new UnsupportedOperationException("Saving Left join with expression(s) is not handled yet.");
        }

        PassageOptional<ID,VALUE> optional = (PassageOptional<ID, VALUE>) getIterator(lj);

        if (Objects.isNull(optional)) {return null;} // again, not saved => no preempted

        Op left = ReturningOpVisitorRouter.visit(this, lj.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, lj.getRight());

        if (Objects.isNull(left) && Objects.isNull(right)) {
            return null;
        }

        // If the optional part exists
        if (optional.hasOptionalPart()) {
            // same as a join. Therefore, if there are no results in the optional part,
            // it returns no results overall, as expected.
            if (Objects.isNull(left)) {

//                if (right instanceof OpProject project) {
//                    return FlattenUnflatten.unflattenUnion(Collections.singletonList(project.getSubOp()));
//                }
//                 return FlattenUnflatten.unflattenUnion(Collections.singletonList(right));

                return FlattenUnflatten.unflattenUnion(
                        Collections.singletonList(OpCloningUtil.clone(lj, lj.getLeft(), right)));
            }
            return FlattenUnflatten.unflattenUnion(Arrays.asList(right,
                    OpCloningUtil.clone(lj, left, lj.getRight()))); // but here, it's an optional still
        }


        if (Objects.isNull(left)) {
            return FlattenUnflatten.unflattenUnion(Collections.singletonList(
                    optional.pause(right) // the right needs mandatory bindings
            ));
        }

        Op rest = OpLeftJoin.create(left, lj.getRight(), ExprList.emptyList);

        if (Objects.isNull(right)) {return rest;}

        return FlattenUnflatten.unflattenUnion(Arrays.asList(
            optional.pause(right), // the right needs mandatory bindings
            rest
        ));
    }

    @Override
    public Op visit(OpGroup groupBy) {
        PassageCount<ID,VALUE> it = (PassageCount<ID, VALUE>) getIterator(groupBy);
        if (Objects.isNull(it)) return null;

        return groupBy;
    }
}
