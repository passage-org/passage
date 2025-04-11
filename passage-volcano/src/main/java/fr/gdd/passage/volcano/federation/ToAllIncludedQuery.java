package fr.gdd.passage.volcano.federation;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.passage.commons.transforms.BGP2Triples;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;

import java.util.Objects;

/**
 * The input for this is a normal query that needs to be performed as a federated
 * query. The query will be modified to include source selection within it.
 * *
 * In other terms, contrarily to state-of-the-art queries that statically provide
 * a source assignment query to be executed, it creates an all-included query that
 * is self-contained. Of course, it requires a different kind of optimizations.
 */
public class ToAllIncludedQuery extends ReturningOpBaseVisitor {

    // TODO for now it's remote, but eventually, allow it to be local, i.e.,
    //      the dataset to query is embedded in the JVM.
    final Node remoteURI;
    final ToSourceAssignmentQuery toSourceAssignmentQuery;
    final boolean silent = true;
    final boolean allLocal = true; // useful for testing, since we don't need actual endpoints

    public ToAllIncludedQuery() {
        this.remoteURI = null;
        this.toSourceAssignmentQuery = new ToSourceAssignmentQuery();
    }

    public ToAllIncludedQuery(String remoteURI) {
        this.remoteURI = Objects.isNull(remoteURI) ? null: NodeFactory.createURI(remoteURI);
        this.toSourceAssignmentQuery = new ToSourceAssignmentQuery(remoteURI);
    }

    public Op create (Op op) {
        Op toProcess = new BGP2Triples().visit(op);
        Op ssq = toSourceAssignmentQuery.create(toProcess);
        return OpJoin.create(ssq, visit(toProcess));
    }

    @Override
    public Op visit(OpTriple triple) {
        // for each triple, we associate its source selection query.
        Var graphName = toSourceAssignmentQuery.getGraphName(triple);
        // TODO double check if we actually need a ASK or if its redundant with subqueries
        return allLocal ?
                new OpGraph(graphName, triple):
                new OpService(graphName, triple, silent);
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
            sequence.add(visit(new OpTriple(triple)));
        });
        return sequence;
    }

    @Override
    public Op visit(OpLeftJoin lj) {
        Op ssq = toSourceAssignmentQuery.create(OpJoin.create(lj.getLeft(), lj.getRight()));
        return OpLeftJoin.createLeftJoin(this.visit(lj.getLeft()),
                OpJoin.create(ssq, this.visit(lj.getRight())), lj.getExprs());
    }
}
