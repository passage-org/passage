package fr.gdd.passage.volcano.transforms;

import fr.gdd.jena.utils.FlattenUnflatten;
import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadBlock;
import org.apache.jena.sparql.core.QuadPattern;

import java.util.ArrayList;
import java.util.List;

/**
 * Only create quad patterns with adjacent Quads, since some `Op` are order
 * sensitive, such as `OpExtend`.
 * TODO take into account order insensitive vs order sensitive operators.
 */
public class Quad2Patterns extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpJoin join) {
        List<Op> ops = FlattenUnflatten.flattenJoin(join);
        List<Op> groupQuads = groupQuads(new ArrayList<>(), new QuadPattern(), ops);
        return BGP2Triples.asJoins(groupQuads);
    }

    /**
     * @param built The list of operators being built.
     * @param current The current QuadPattern being built.
     * @param ops The rest of operators to examine.
     * @return A list of operators where adjacent OpQuad are factorized into OpQuadBlocks.
     */
    private static List<Op> groupQuads(List<Op> built, QuadPattern current, List<Op> ops) {
        if (ops.isEmpty() && current.isEmpty()) return built;
        if (ops.isEmpty() && !current.isEmpty()) {
            built.add(new OpQuadBlock(current));
            return built;
        }

        switch (ops.getFirst()) {
            case OpQuad quad -> {
                current.add(quad.getQuad());
                return groupQuads(built, current, ops.subList(1, ops.size()));
            }
            default -> {
                if (!current.isEmpty()) {
                    built.add(new OpQuadBlock(current));
                    current = new QuadPattern();
                }
                built.add(ops.getFirst());
                return groupQuads(built, current, ops.subList(1, ops.size()));
            }
        }

    }


//    /**
//     * @param op The operator to retrieve quad and rest.
//     * @return All quads directly linked together by JOIN operators. Get the rest pf
//     *         operators as well.
//     */
//    private static Pair<List<Quad>, List<Op>> getAllQuadsAndRest(Op op) {
//        if (op instanceof OpQuad) {
//            List<Quad> quads = new ArrayList<>();
//            quads.add(((OpQuad) op).getQuad());
//            return new ImmutablePair<>(quads, new ArrayList<>());
//        } else if (op instanceof OpJoin join) {
//            var quadsAndRestLeft = getAllQuadsAndRest(join.getLeft());
//            var quadsAndRestRight = getAllQuadsAndRest(join.getRight());
//            var quads = new ArrayList<>(quadsAndRestLeft.getLeft());
//            quads.addAll(quadsAndRestRight.getLeft());
//            var rest = new ArrayList<>(quadsAndRestLeft.getRight());
//            rest.addAll(quadsAndRestRight.getRight());
//            return new ImmutablePair<>(quads, rest);
//        }
//        return new ImmutablePair<>(new ArrayList<>(), Collections.singletonList(op));
//    }
}
