package fr.gdd.passage.volcano.resume;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSlice;

import java.util.ArrayList;
import java.util.List;

/**
 * "Due to the bottom-up nature of SPARQL query evaluation,
 * the subqueries are evaluated logically first, and the results
 * are projected up to the outer query."
 *
 * Since our subqueries are one triple pattern only, we still can
 * proceed with tuple-at-a-time evaluation though.
 */
public class Subqueries2LeftOfJoins extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpJoin join) {
        List<Op> ops = FlattenUnflatten.flattenJoin(join);
        List<Op> subqueries = ops.stream().filter(o -> o instanceof OpSlice).toList();
        List<Op> rest = ops.stream().filter(o -> ! (o instanceof OpSlice)).toList();
        List<Op> ordered = new ArrayList<>(subqueries);
        ordered.addAll(rest);
        return BGP2Triples.asJoins(ordered);
    }
}
