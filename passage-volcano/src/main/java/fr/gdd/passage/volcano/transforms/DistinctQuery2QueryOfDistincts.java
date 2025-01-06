package fr.gdd.passage.volcano.transforms;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.passage.volcano.querypatterns.IsDistinctableQuery;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.VarUtils;

import java.util.Objects;
import java.util.Set;

/**
 * Equivalence transformation to process distinct using well known
 * operators that are preemptable and more efficient. Most of the time copy.
 * But the distinct
 * queries such as:
 * SELECT DISTINCT ?address WHERE {
 *     ?person <http://address> ?address .
 *     ?person <http://own> ?animal
 * }
 * Is transformed to:
 * SELECT DISTINCT ?address WHERE {
 *  {SELECT DISTINCT ?address ?person WHERE {
 *      ?person <http://address> ?address .
 *  }}
 *  {SELECT DISTINCT ?person WHERE {
 *      ?person <http://own> ?animal .
 *  }}
 * }
 */
public class DistinctQuery2QueryOfDistincts extends ReturningOpBaseVisitor {

    IsDistinctableQuery isDistinctableQuery = new IsDistinctableQuery();

    @Override
    public Op visit(OpDistinct distinct) {
        if (isDistinctableQuery.visit((Op) distinct)) {
            return super.visit(distinct.getSubOp());
        }
        throw new UnsupportedOperationException("Cannot rewrite the query into query of distincts that allow continuation.");
    }

    @Override
    public Op visit(OpBGP bgp) {
        // We assume here that the bgp is already ordered.
        return bgp.getPattern().getList().stream().map(t -> (Op) new OpTriple(t)).reduce(null,
                (left, right) -> {
                    if (Objects.isNull(left)) {
                        return transformIntoDistinct(right);
                    } else {
                        return OpJoin.create(left, transformIntoDistinct(right));
                    }
                });
    }

    private Op transformIntoDistinct(Op op) {
        if  (Objects.isNull(isDistinctableQuery.project)) {
            return op;
        }

        OpTriple opTriple = (OpTriple) op;
        Set<Var> vars = VarUtils.getVars(opTriple.getTriple());
        Integer sizeBefore = vars.size();
        vars.retainAll(isDistinctableQuery.project.getVars());
        if (sizeBefore != vars.size()) {
            return new OpDistinct(new OpProject(opTriple, vars.stream().toList()));
        }

        // all vars are projected
        return opTriple;

    }
}
