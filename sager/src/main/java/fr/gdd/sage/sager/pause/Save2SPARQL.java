package fr.gdd.sage.sager.pause;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.PtrMap;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.iterators.SagerDistinct;
import fr.gdd.sage.sager.iterators.SagerOptional;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import fr.gdd.sage.sager.iterators.SagerUnion;
import fr.gdd.sage.sager.resume.IsSkippable;
import fr.gdd.sage.sager.resume.Subqueries2LeftOfJoins;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.expr.ExprList;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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

    public Op getRoot() {return root;}

    /* **************************************************************************** */

    public Op save(Op caller) {
        this.caller = caller;
        Op saved = ReturningOpVisitorRouter.visit(this, root);
        saved = Objects.isNull(saved) ? saved : ReturningOpVisitorRouter.visit(new Triples2BGP(), saved);
        saved = Objects.isNull(saved) ? saved : ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), saved);
        return saved;
    }

    @Override
    public Op visit(OpProject project) {
        Op subop =  ReturningOpVisitorRouter.visit(this, project.getSubOp());
        return Objects.isNull(subop) ? null : OpCloningUtil.clone(project, subop);
    }

    @Override
    public Op visit(OpDistinct distinct) {
        SagerDistinct<ID, VALUE> it = (SagerDistinct<ID, VALUE>) op2it.get(distinct);

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
        SagerScanFactory<ID, VALUE> it = (SagerScanFactory<ID, VALUE>) op2it.get(triple);

        // If the scan iterator is not registered, it should not be preempted
        if (Objects.isNull(it)) {return null;}

        // In `it.preempt()`, it returns null if the scan does not have a next, i.e.,
        // if the can is done. This aims at removing the branches of the plans that are
        // done, finished. Otherwise, they would pollute the preempted plan forever.
        // It creates a subquery including bindings with `BIND AS`, then the triple
        // pattern. Then wraps it all with `OFFSET`.
        return it.preempt();
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
        SagerUnion<ID,VALUE> u = (SagerUnion<ID,VALUE>) op2it.get(union);

        // not executed yet, otherwise would exist. So no need to preempt it.
        if (Objects.isNull(u)) {return null;}

        if (u.onLeft()) {  // preempt the left, copy the right for later
            Op left = ReturningOpVisitorRouter.visit(this, union.getLeft());
            return FlattenUnflatten.unflattenUnion(Arrays.asList(left, union.getRight()));
        } else { // on right: remove left part of union
            return  ReturningOpVisitorRouter.visit(this, union.getRight());
        }
    }

    @Override
    public Op visit(OpSlice slice) {
        IsSkippable isSkippableVisitor = new IsSkippable();
        Boolean isSkippable = ReturningOpVisitorRouter.visit(isSkippableVisitor, slice);
        if (isSkippable) {
            // behaves as if it does not exist since the tp is interpreted as tp with skip.
            // If need be, the tp will add the slice OFFSET itself.
            return ReturningOpVisitorRouter.visit(this, isSkippableVisitor.getOpTriple());
        }
        throw new UnsupportedOperationException("TODO OpSlice cannot be saved right now."); // TODO
    }

    @Override
    public Op visit(OpExtend extend) { // cloned
        // return OpCloningUtil.clone(extend, ReturningOpVisitorRouter.visit(this, extend.getSubOp()));
        return null;
    }

    @Override
    public Op visit(OpTable table) {
        if (!table.isJoinIdentity()){
            throw new UnsupportedOperationException("OpTable not implemented when not join identity…");
        }
        return table;
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

        SagerOptional<ID,VALUE> optional = (SagerOptional<ID, VALUE>) op2it.get(lj);

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
                if (Objects.isNull(right)) {
                    return null;
                }
                return FlattenUnflatten.unflattenUnion(
                        Collections.singletonList(OpCloningUtil.clone(lj, lj.getLeft(), right)));
            }
            return FlattenUnflatten.unflattenUnion(Arrays.asList(right,
                    OpCloningUtil.clone(lj, left, lj.getRight()))); // but here, it's an optional still
        }

//        // But it might mean that the optional part is not executed yet
//        if (Objects.isNull(left) && Objects.isNull(right)) {
//            return null;
//        }

        if (Objects.isNull(left)) {
            return FlattenUnflatten.unflattenUnion(Arrays.asList(
                    optional.preempt(right) // the right needs mandatory bindings
            ));
        }

        Op rest = OpLeftJoin.create(left, lj.getRight(), ExprList.emptyList);

        if (Objects.isNull(right)) {return rest;}

        return FlattenUnflatten.unflattenUnion(Arrays.asList(
            optional.preempt(right), // the right needs mandatory bindings
            rest
        ));
    }
}
