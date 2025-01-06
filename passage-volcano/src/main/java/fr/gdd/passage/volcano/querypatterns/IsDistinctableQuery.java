package fr.gdd.passage.volcano.querypatterns;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.VarUtils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A DISTINCT of a subquery may or may not be rewritable as a series of
 * DISTINCT of a series of triple/quad pattern.
 * The test consists in checking in
 * TODO for tp
 * TODO for bgp
 */
public class IsDistinctableQuery extends ReturningOpVisitor<Boolean> {

    public OpProject project;
    public Op0 tripleOrQuad;

    @Override
    public Boolean visit(Op op) {
        try {
            return super.visit(op);
        } catch (UnsupportedOperationException e) {
            return false;
        }
    }

    @Override
    public Boolean visit(OpDistinct distinct) {
        return super.visit(distinct.getSubOp());
    }

    @Override
    public Boolean visit(OpProject project) {
        this.project = project;
        return super.visit(project.getSubOp());
    }

    @Override
    public Boolean visit(OpTriple triple) {
//        if (Objects.isNull(project)) {
//            project = new OpProject(triple, VarUtils.getVars(triple.getTriple()).stream().toList());
//        }
        tripleOrQuad = triple;
        return true;
    }

    @Override
    public Boolean visit(OpQuad quad) {
//        if (Objects.isNull(project)) {
//            List<Var> vars = new ArrayList<>(VarUtils.getVars(quad.getQuad().asTriple()));
//            if (quad.getQuad().getGraph().isVariable()) {
//                vars.add(Var.alloc(quad.getQuad().getGraph()));
//            }
//            project = new OpProject(quad, vars);
//        }
        tripleOrQuad = quad;
        return true;
    }

    @Override
    public Boolean visit(OpBGP bgp) {
        if (Objects.isNull(project)) return true; // everything is linked since everything is distinct

        Set<Var> setVariables = new HashSet<>();

        for (Triple triple : bgp.getPattern()) {
            Set<Var> varsOfTriple = VarUtils.getVars(triple);
            varsOfTriple.retainAll(project.getVars());

            if (varsOfTriple.isEmpty()) {
                return false;
            }

            varsOfTriple.retainAll(setVariables);
            if (!setVariables.isEmpty() && varsOfTriple.isEmpty()) {
                return false;
            }
            setVariables.addAll(VarUtils.getVars(triple));
        }

        return true;
        // TODO check if all triple/quad patterns are linked together by variables
        //      that happen to be in the DISTINCT clause.
        // throw new UnsupportedOperationException("DISTINCT BGP Not supported yet.");
    }
}
