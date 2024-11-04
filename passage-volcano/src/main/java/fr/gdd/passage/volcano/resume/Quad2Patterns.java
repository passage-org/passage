package fr.gdd.passage.volcano.resume;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadBlock;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadPattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Quad2Patterns extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpJoin join) {
        var quadsAndRest = getAllQuadsAndRest(join);
        QuadPattern quads = new QuadPattern();
        quadsAndRest.getLeft().forEach(quads::add);
        if (!quads.isEmpty() && !quadsAndRest.getRight().isEmpty()) {
            return OpJoin.create(new OpQuadBlock(quads), BGP2Triples.asJoins(quadsAndRest.getRight()));
        } else if (!quads.isEmpty()) {
            return new OpQuadBlock(quads);
        } else {
            return BGP2Triples.asJoins(quadsAndRest.getRight());
        }
    }

    /**
     * @param op The operator to retrieve quad and rest.
     * @return All quads directly linked together by JOIN operators. Get the rest pf
     *         operators as well.
     */
    private static Pair<List<Quad>, List<Op>> getAllQuadsAndRest(Op op) {
        if (op instanceof OpQuad) {
            List<Quad> quads = new ArrayList<>();
            quads.add(((OpQuad) op).getQuad());
            return new ImmutablePair<>(quads, new ArrayList<>());
        } else if (op instanceof OpJoin join) {
            var quadsAndRestLeft = getAllQuadsAndRest(join.getLeft());
            var quadsAndRestRight = getAllQuadsAndRest(join.getRight());
            var quads = new ArrayList<>(quadsAndRestLeft.getLeft());
            quads.addAll(quadsAndRestRight.getLeft());
            var rest = new ArrayList<>(quadsAndRestLeft.getRight());
            rest.addAll(quadsAndRestRight.getRight());
            return new ImmutablePair<>(quads, rest);
        }
        return new ImmutablePair<>(new ArrayList<>(), Collections.singletonList(op));
    }
}
