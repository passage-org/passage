package fr.gdd.passage.volcano.federation;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;

import java.util.List;

/**
 * The input for this is a normal query that needs to be performed as a federated
 * query. The query will be modified to include source selection within it.
 */
public class ToAllIncludedQuery extends ReturningOpBaseVisitor {

    // TODO for now it's remote, but eventually, allow it to be local, i.e.,
    //      the dataset to query is embedded in the JVM.
    final Node remoteURI;
    int nbGraphs = 0;
    ToSummaryPattern toSummaryPattern = new ToSummaryPattern();

    public ToAllIncludedQuery(String remoteURI) {
        this.remoteURI = NodeFactory.createURI(remoteURI);
    }

    public Var allocateGraphName() {
        return Var.alloc("_g"+(++this.nbGraphs));
    }

    @Override
    public Op visit(OpTriple triple) {
        // for each triple, we associate its source selection query.
        Var graphName = this.allocateGraphName();
        Op left = new OpService(
                remoteURI,
                new OpQuadPattern(graphName, BasicPattern.wrap(List.of(toSummaryPattern.toSummaryTriple(triple.getTriple(), 1)))),
                true);
        Op right = new OpService(
                graphName, triple, true
        );
        // TODO double check if we actually need a ASK or if its redundant with subqueries
        return OpJoin.create(left, right);
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
            OpJoin toUnwrap = (OpJoin) this.visit(new OpTriple(triple));
            sequence.add(toUnwrap.getLeft());
            sequence.add(toUnwrap.getRight());
        });
        return sequence;
    }
}
