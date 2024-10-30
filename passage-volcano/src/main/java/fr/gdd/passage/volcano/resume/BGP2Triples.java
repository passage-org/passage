package fr.gdd.passage.volcano.resume;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Preempted iterators are identified by their `Op` which is either
 * `OpTriple` or `OpQuad`.
 * Therefore, `OpBGP`, `OpQuadBlock`, and `OpQuadPattern` must be
 * unfolded into these more simple elements.
 */
public class BGP2Triples extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpBGP bgp) {
        return switch (bgp.getPattern().getList().size()) {
            case 0 -> throw new QueryExecException();
            case 1 -> new OpTriple(bgp.getPattern().get(0));
            default ->
                asJoins(bgp.getPattern().getList().stream().map(OpTriple::new).collect(Collectors.toList()));
        };
    }

    @Override
    public Op visit(OpSequence sequence) {
        OpSequence transformedInside = (OpSequence) super.visit(sequence);
        return asJoins(transformedInside.getElements());
    }

    public static Op asJoins(List<Op> ops) {
        if (ops.isEmpty()) {
            return null;
        }
        if (ops.size() == 1) {
            return ops.get(0);
        }
        Op left = ops.get(0);
        for (int i = 1; i < ops.size(); ++i) {
            Op right = ops.get(i);
            left = OpJoin.create(left, right);
        }
        return left;
    }

    public static Op asSequence(List<Op> ops) {
        OpSequence sequence = OpSequence.create();
        for (Op t : ops) {
            sequence.add(t);
        }
        return sequence;
    }

}
