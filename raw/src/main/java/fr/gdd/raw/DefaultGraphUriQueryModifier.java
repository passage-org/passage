package fr.gdd.raw;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import org.apache.commons.lang.ObjectUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class DefaultGraphUriQueryModifier extends ReturningArgsOpVisitor<Op, ExecutionContext> {
    public Op visit(OpTriple opTriple, ExecutionContext executionContext){
        List<Node> dfgs = extractGraphs(executionContext);

        // no default-graph-uri -> no changes to be made
        if (dfgs.isEmpty()) return opTriple;

        // TODO
        if(dfgs.size() > 1) throw new UnsupportedOperationException("Muliple default graphs is not implemented yet");

        Node dfg = dfgs.get(0);
        return new OpQuad(new Quad(dfg, opTriple.getTriple()));

    }

    public Op visit(OpQuad opQuad, ExecutionContext executionContext) {
        List<Node> dfgs = extractGraphs(executionContext);

        // no default-graph-uri -> no changes to be made
        if (dfgs.isEmpty()) return opQuad;

        // TODO
        if(dfgs.size() > 1) throw new UnsupportedOperationException("Muliple default graphs is not implemented yet");

        Node dfg = dfgs.get(0);
        // we force the default graph ONLY if a graph isn't already specified
        // should we force default graph when graph is just not binded as well?
        Node graphToVisit = (Node) ObjectUtils.defaultIfNull(opQuad.getQuad().getGraph(), dfg);
        return new OpQuad(new Quad(graphToVisit, opQuad.getQuad().asTriple()));
    }

    public Op visit(OpJoin opJoin, ExecutionContext executionContext) {
        Op left = ReturningArgsOpVisitorRouter.visit(this, opJoin.getLeft(), executionContext);
        Op right = ReturningArgsOpVisitorRouter.visit(this, opJoin.getRight(), executionContext);
        Op newJoin = OpJoin.create(left, right);
        return newJoin;
    }

    private List<Node> extractGraphs(ExecutionContext executionContext){
        Set<Node> defaultGraphs = (Set) executionContext.getDataset().getContext().get(ARQConstants.symDatasetDefaultGraphs);

        return Objects.isNull(defaultGraphs) ? Collections.EMPTY_LIST : defaultGraphs.stream().toList();
    }
}
