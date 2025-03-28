package fr.gdd.passage.commons.transforms;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadBlock;

public class Patterns2Quad extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpQuadBlock block) {
        return BGP2Triples.asJoins(block.getPattern().getList().stream().map(q -> (Op) new OpQuad(q)).toList());
    }
}
