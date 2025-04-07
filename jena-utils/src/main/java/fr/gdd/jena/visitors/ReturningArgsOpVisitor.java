package fr.gdd.jena.visitors;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

/**
 * The visitor must implement this interface. Added value compared
 * to the default visitor {@link org.apache.jena.sparql.algebra.OpVisitor}:
 * it returns a type AND it passes arguments downstream.
 *
 * Remember to use the {@link ReturningArgsOpVisitorRouter} to call downstream visitors.
 * @param <R> The type of the object returned.
 * @param <A> The type of the argument to be passed.
 */
public interface ReturningArgsOpVisitor<R, A> {
    default public R visit(Op op, A args) {return ReturningArgsOpVisitorRouter.visit(this, op, args);}

    default R visit(OpService req, A args) {throw new UnsupportedOperationException("OpService");}

    default R visit(OpTriple triple, A args) {throw new UnsupportedOperationException("OpTriple");}
    default R visit(OpBGP bgp, A args) {throw new UnsupportedOperationException("OpBGP");}
    default R visit(OpQuad quad, A args) {throw new UnsupportedOperationException("OpQuad");}
    default R visit(OpGraph graph, A args) {throw new UnsupportedOperationException("OpGraph");}
    default R visit(OpQuadBlock block, A args) {throw new UnsupportedOperationException("OpQuadBlock");}
    default R visit(OpSequence sequence, A args) {throw new UnsupportedOperationException("OpSequence");}
    default R visit(OpTable table, A args) {throw new UnsupportedOperationException("OpTable");}
    default R visit(OpLeftJoin lj, A args) {throw new UnsupportedOperationException("OpLeftJoin");}
    default R visit(OpConditional cond, A args) {throw new UnsupportedOperationException("OpConditional");}
    default R visit(OpFilter filter, A args) {throw new UnsupportedOperationException("OpFilter");}
    default R visit(OpUnion union, A args) {throw new UnsupportedOperationException("OpUnion");}
    default R visit(OpJoin join, A args) {throw new UnsupportedOperationException("OpJoin");}

    default R visit(OpDistinct distinct, A args) {throw new UnsupportedOperationException("OpDistinct");}
    default R visit(OpSlice slice, A args) {throw new UnsupportedOperationException("OpSlice");}
    default R visit(OpOrder orderBy, A args)  {throw new UnsupportedOperationException("OpOrder");}
    default R visit(OpProject project, A args) {throw new UnsupportedOperationException("OpProject");}
    default R visit(OpGroup groupBy, A args) {throw new UnsupportedOperationException("OpGroup");}

    default R visit(OpExtend extend, A args) {throw new UnsupportedOperationException("OpExtend");}
}
