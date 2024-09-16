package fr.gdd.passage.volcano.optimizers;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpProject;

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
public class DistinctBGP2JoinsOfDistincts extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpDistinct distinct) {
        // TODO TODO TODO
        if (distinct.getSubOp() instanceof OpBGP || // Project *
                (distinct.getSubOp() instanceof OpProject project && // Project Vars
                        project.getSubOp() instanceof OpBGP bgp)) {
            throw new UnsupportedOperationException("TODO"); // TODO TODO
        }
        return super.visit(distinct); // otherwise copy
    }
}
