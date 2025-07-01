package fr.gdd.passage.commons.transforms;

import fr.gdd.jena.utils.OpCloningUtil;
import fr.gdd.jena.visitors.ReturningOpCustomBaseVisitor;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpGraph;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.engine.ExecutionContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * When `default-graph-uri` is set in the HTTP request, it changes the meaning
 * of the SPARQL query to apply only on the targeted graph. This visitor modifies
 * the query accordingly.
 */
public class DefaultGraphURIQueryWrapper extends ReturningOpCustomBaseVisitor {

    public DefaultGraphURIQueryWrapper(ExecutionContext context) {
        super();
        Function<Op, Op> function = op -> op;

        List<Node> dfgs = extractGraphs(context);

        // TODO
        if(dfgs.size() > 1) throw new UnsupportedOperationException("Multiple default graphs is not implemented yet.");

        // no default-graph-uri -> no changes to be made
        if (dfgs.isEmpty()) function = op -> op;
        else function = op -> new OpGraph(dfgs.getFirst(), op);

        setFunction(function);
    }

    @Override
    public Op visit(OpProject opProject){
        return OpCloningUtil.clone(opProject, this.visit(opProject.getSubOp()));
    }

    /* ********************************** UTILS *************************************** */

    private static List<Node> extractGraphs(ExecutionContext executionContext){
        Set<Node> defaultGraphs = executionContext.getDataset().getContext().get(ARQConstants.symDatasetDefaultGraphs);

        return Objects.isNull(defaultGraphs) ? Collections.EMPTY_LIST : defaultGraphs.stream().toList();
    }

}
