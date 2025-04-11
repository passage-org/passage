package fr.gdd.passage.volcano.federation;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.ExprUtils;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;

public class ToSourceAssignmentQuery extends ReturningOpBaseVisitor {

    final IdentityHashMap<Op, Var> op2graph = new IdentityHashMap<>();
    final ToSummaryPattern toSummaryPattern = new ToSummaryPattern();
    final Node remoteURI;
    final boolean silent = true;
    final IdentityHashMap<Var, Var> graph2graph = new IdentityHashMap<>(); // if modified with transforms
    final String transformFrom =  "$"; // TODO configurable
    final String transformTo =  "/";

    public ToSourceAssignmentQuery() {
        this.remoteURI = null;
    }

    public ToSourceAssignmentQuery(String remoteURI) {
        this.remoteURI = Objects.isNull(remoteURI) ? null: NodeFactory.createURI(remoteURI);
    }

    public Var allocateGraphName() {
        return Var.alloc("_g"+(this.op2graph.size()));
    }

    public Var getGraphName(Op op) {
        Var graphName = op2graph.get(op);
        if (Objects.nonNull(graphName) && graph2graph.containsKey(graphName)) {
            return graph2graph.get(graphName);
        } else {
            return graphName;
        }
    }

    public Op create(Op op) {
        Op root = visit(op); // done before to retrieve variables

        // TODO include transformation of graphName to endpoint (i.e. `modify` argument)
        //      otherwise, we need to assume that the summary is always up to date with
        //      endpoints, which should be rare.
        if (Objects.nonNull(transformFrom) && Objects.nonNull(transformTo)) { // TODO configurable
            OpSequence transforms = OpSequence.create();
            transforms.add(root);

            op2graph.values().forEach(graph -> {
                Var transformedName = graph2graph.computeIfAbsent(graph, k -> Var.alloc("_"+graph.getName()));
                transforms.add(OpExtend.create(OpTable.unit(), transformedName,
                        ExprUtils.parse(String.format("REPLACE(%s, \"%s\", \"%s\")",
                                graph, transformFrom, transformTo))));

            });
            root = transforms;
        }
        Op projectAndDistinct = OpDistinct.create(new OpProject(root, op2graph.keySet().stream().map(this::getGraphName).toList()));
        return Objects.nonNull(remoteURI) ?
                new OpService(remoteURI, projectAndDistinct, silent) :
                projectAndDistinct; // otherwise it's local
    }

    /* ******************************* OPERATORS ******************************* */

    @Override
    public Op visit(OpTriple triple) {
        Var graphName = op2graph.computeIfAbsent(triple, k -> this.allocateGraphName());
        return new OpQuadPattern(graphName, BasicPattern.wrap(List.of(toSummaryPattern.toSummaryTriple(triple.getTriple(), 1))));
    }

    @Override
    public Op visit(OpQuad quad) {
        // because the summary do not provide ways to get quads yet
        // TODO create a Summary interface generic enough to provide it.
        //      since it could be possible, eg. using
        throw new UnsupportedOperationException("Quads are not supported just yet…");
    }

    @Override
    public Op visit(OpBGP bgp) {
        OpSequence sequence = OpSequence.create();
        bgp.getPattern().getList().forEach(triple -> {
            sequence.add(this.visit(new OpTriple(triple)));  // TODO add filters for ASK
        });
        return sequence;
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        // we only visit left side, since the right part is not mandatory,
        // an additional processing is required that depends on the left.
        return visit(lj.getLeft());
    }
}
