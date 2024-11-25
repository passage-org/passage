package fr.gdd.passage.commons.transforms;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * When `default-graph-uri` is set in the HTTP request, it changes the meaning
 * of the SPARQL query to apply only on the targeted graph. This visitor modifies
 * the query accordingly.
 */
public class DefaultGraphUriQueryModifier extends ReturningOpBaseVisitor {

    private final ExecutionContext context;

    public DefaultGraphUriQueryModifier(ExecutionContext context) {
        this.context = context;
    }

    @Override
    public Op visit(OpTriple opTriple){
        List<Node> dfgs = extractGraphs(context);

        // no default-graph-uri -> no changes to be made
        if (dfgs.isEmpty()) return opTriple;

        // TODO
        if(dfgs.size() > 1) throw new UnsupportedOperationException("Multiple default graphs is not implemented yet.");

        Node dfg = dfgs.get(0);
        return new OpQuad(new Quad(dfg, opTriple.getTriple()));

    }

    @Override
    public Op visit(OpQuad opQuad) {
        List<Node> dfgs = extractGraphs(context);

        // no default-graph-uri -> no changes to be made
        if (dfgs.isEmpty()) return opQuad;

        // TODO
        if(dfgs.size() > 1) throw new UnsupportedOperationException("Multiple default graphs is not implemented yet.");

        Node dfg = dfgs.get(0);
        // we force the default graph ONLY if a graph isn't already specified
        // should we force default graph when graph is just not binded as well?
        Node graphToVisit = ObjectUtils.defaultIfNull(opQuad.getQuad().getGraph(), dfg);
        return new OpQuad(new Quad(graphToVisit, opQuad.getQuad().asTriple()));
    }

    /* ********************************** UTILS *************************************** */

    private static List<Node> extractGraphs(ExecutionContext executionContext){
        Set<Node> defaultGraphs = executionContext.getDataset().getContext().get(ARQConstants.symDatasetDefaultGraphs);

        return Objects.isNull(defaultGraphs) ? Collections.EMPTY_LIST : defaultGraphs.stream().toList();
    }

}
