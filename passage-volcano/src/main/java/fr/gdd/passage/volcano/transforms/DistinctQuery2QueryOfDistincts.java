package fr.gdd.passage.volcano.transforms;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.passage.volcano.querypatterns.IsDistinctableQuery;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;

import java.util.Objects;

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
    public Op visit(Op op) {
        return super.visit(op);
    }

    @Override
    public Op visit(OpDistinct distinct) {
        // The transformation only triggers when a subquery is detected.
        if (isDistinctableQuery.visit((Op) distinct)) {
            return new ApplyDistinctQueries(isDistinctableQuery.project).visit(distinct.getSubOp());
        }
        throw new UnsupportedOperationException("Cannot rewrite the query into query of distincts that allow continuation.");
    }


    /**
     * Actually apply the transformation to bgp in the distinct subquery.
     */
    static class ApplyDistinctQueries extends ReturningOpBaseVisitor {

        final OpProject project;

        public ApplyDistinctQueries(OpProject project) { this.project = project; }

        @Override
        public Op visit(OpSlice slice) {
            return slice;
        }

        @Override
        public Op visit(OpBGP bgp) {
            // We assume here that the bgp is already ordered.
            return bgp.getPattern().getList().stream().map(t -> (Op) new OpTriple(t)).reduce(null,
                    (left, right) -> Objects.isNull(left) ?
                            transformIntoDistinct(right) :
                            OpJoin.create(left, transformIntoDistinct(right)));
        }

        // TODO OpQuadPattern

        private Op transformIntoDistinct(Op op) {
            if (Objects.isNull(project)) { // SELECT DISTINCT * {…}
                return new OpDistinct(op);
            }

            // otherwise // SELECT
            // OpTriple opTriple = (OpTriple) op;
//            Set<Var> vars = VarUtils.getVars(opTriple.getTriple());
//            Integer sizeBefore = vars.size();
//            vars.retainAll(project.getVars());

            // we clone the full project because injected bindings may include needed variables
            // so they are projected still.
            return new OpDistinct(OpCloningUtil.clone(project, op));

//        if (sizeBefore != vars.size()) {
//            return new OpDistinct(new OpProject(opTriple, vars.stream().toList()));
//        }
//
//        // all vars are projected
//        return opTriple;
        }

    }
}
