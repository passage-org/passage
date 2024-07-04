package fr.gdd.sage.rawer.subqueries;

import fr.gdd.jena.visitors.ReturningOpVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.ExprLib;

import java.util.Set;

/**
 * From an original query, and a set of binding to bind the subquery, builds
 * a COUNT subquery to know the number of elements of targeted binding including
 * duplicates.
 */
public class CountSubqueryBuilder<ID,VALUE> extends ReturningOpVisitor<Op> {

    final BackendBindings<ID,VALUE> bindings;
    final Set<Var> vars;
    final CacheId<ID,VALUE> cache;

    public CountSubqueryBuilder (Backend<ID,VALUE,?> backend, BackendBindings<ID, VALUE> bindings, Set<Var> vars) {
        this.bindings = bindings;
        this.vars = vars;
        this.cache = new CacheId<>(backend);
    }

    public Op build (Op root) {
        for (Var toBind : vars) {
            // Important note: here we have a placeholder because we already have the id, but we
            // don't need the actual value as a string, since we stay in the same engine overall.
            // So this improves (i) performance as we don't need to retrieve the actual value in the database;
            // and (ii) reliability as we get the id from the database, we know for sure it exists as is,
            // while another round of translation would not guarantee it.
            // However, if we happen to output the subquery, it would display placeholders that are meaningless
            // and the query would not return anything. This could be an issue for Sage.
            Node valueAsNode = NodeFactory.createLiteralString("PLACEHOLDER_" + toBind.toString());
            cache.register(valueAsNode, bindings.get(toBind).getId());
            // right = OpJoin.create(OpExtend.extend(OpTable.unit(), toBind, ExprLib.nodeToExpr(valueAsNode)), right);
        }


        return ReturningOpVisitorRouter.visit(this, root);
    }

}
