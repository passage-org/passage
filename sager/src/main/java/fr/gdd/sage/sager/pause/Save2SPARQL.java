package fr.gdd.sage.sager.pause;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.PtrMap;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.iterators.SagerOptional;
import fr.gdd.sage.sager.iterators.SagerScan;
import fr.gdd.sage.sager.iterators.SagerScanFactory;
import fr.gdd.sage.sager.iterators.SagerUnion;
import fr.gdd.sage.sager.resume.IsSkippable;
import fr.gdd.sage.sager.resume.Subqueries2LeftOfJoins;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.*;

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
        Op left = ReturningOpVisitorRouter.visit(this, join.getLeft());
        Op right = ReturningOpVisitorRouter.visit(this, join.getRight());

        if (Objects.isNull(left)) {
            return FlattenUnflatten.unflattenUnion(Collections.singletonList(right));
        }
        return FlattenUnflatten.unflattenUnion(Arrays.asList(right, OpJoin.create(left, join.getRight())));
    }

    @Override
    public Op visit(OpUnion union) {
        SagerUnion<ID,VALUE> u = (SagerUnion<ID,VALUE>) op2it.get(union);
        if (Objects.isNull(u)) { // not executed yet
            return union;
        }

        if (u.onLeft()) {
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
        return OpCloningUtil.clone(extend, ReturningOpVisitorRouter.visit(this, extend.getSubOp()));
    }

    @Override
    public Op visit(OpConditional cond) {
        throw new UnsupportedOperationException("Copy the behavior of OPLeftJoin")
    }

    @Override
    public Op visit(OpLeftJoin lj) {
//        if (Objects.isNull(lj.getExprs()) || lj.getExprs().isEmpty()) {
//            SagerOptional<ID,VALUE> opt = (SagerOptional<ID, VALUE>) op2it.get(lj);
//
//            if (Objects.isNull(opt)) {
//                return lj;
//            }
//
//            if (opt.hasOptionalPart()) {
//                Op left = ReturningOpVisitorRouter.visit(this, lj.getLeft());
//            }
//
//        }
        throw new UnsupportedOperationException("Saving Left join with expression(s) is not handled yet.");
    }
}
