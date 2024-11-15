package fr.gdd.passage.volcano.transforms;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.query.QueryExecException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;

import java.util.stream.Collectors;

/**
 * Distribute the Graph to triple patterns downstream.
 */
public class Graph2Quads extends ReturningOpBaseVisitor {

    @Override
    public Op visit(OpGraph graph) {
        return switch (graph.getSubOp()) {
            case OpTriple opTriple -> new OpQuad(new Quad(graph.getNode(), opTriple.getTriple()));
            case OpBGP bgp-> switch (bgp.getPattern().getList().size()) {
                case 0 -> throw new QueryExecException(); // TODO change to our own exception
                case 1 -> new OpQuad(new Quad(graph.getNode(), bgp.getPattern().get(0)));
                default -> BGP2Triples.asJoins(
                        bgp.getPattern().getList().stream()
                                .map(t -> new OpQuad(new Quad(graph.getNode(), t)))
                                .collect(Collectors.toList()));
            };
            default -> throw new UnsupportedOperationException("OpGraph not well handled for: " + graph.getSubOp());
        };
    }
}
