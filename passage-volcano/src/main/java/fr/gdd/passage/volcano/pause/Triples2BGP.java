package fr.gdd.passage.volcano.pause;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.passage.volcano.resume.BGP2Triples;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.List;

/**
 * Converse operation of {@link BGP2Triples}, just so it looks better.
 */
public class Triples2BGP extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpJoin join) {
        List<Op> ops = FlattenUnflatten.flattenJoin(join);
        List<Triple> triples = ops.stream().filter(o -> o instanceof OpTriple)
                .map(o -> ((OpTriple) o).getTriple()).toList();
        List<Op> rest = ops.stream().filter(o -> ! (o instanceof OpTriple)).toList();
        OpBGP bgp = new OpBGP(BasicPattern.wrap(triples));
        if (!triples.isEmpty() && !rest.isEmpty()) {
            return OpJoin.create(BGP2Triples.asJoins(rest), bgp);
        } else if (!triples.isEmpty()){
            return bgp;
        } else {
            return BGP2Triples.asJoins(rest);
        }
    }

}
