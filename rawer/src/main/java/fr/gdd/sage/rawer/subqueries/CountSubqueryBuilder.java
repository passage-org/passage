package fr.gdd.sage.rawer.subqueries;

import fr.gdd.jena.visitors.ReturningOpBaseVisitor;
import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerConstants;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.aggregate.AggCount;

import java.util.List;
import java.util.Set;

/**
 * From an original query, and a set of binding to bind the subquery, builds
 * a COUNT subquery to know the number of elements of targeted binding including
 * duplicates.
 */
public class CountSubqueryBuilder<ID,VALUE> extends ReturningOpBaseVisitor {

    final BackendBindings<ID,VALUE> bindings;
    final Set<Var> vars;
    final CacheId<ID,VALUE> cache;

    Var resultVar; // to retrieve the result in the COUNT subquery

    public CountSubqueryBuilder (Backend<ID,VALUE,?> backend, BackendBindings<ID, VALUE> bindings, Set<Var> vars) {
        this.bindings = bindings;
        this.vars = vars;
        this.cache = new CacheId<>(backend);
    }

    public Var getResultVar() { return resultVar;}

    public CacheId<ID, VALUE> getCache() { return cache;}

    public Op build (String queryAsString) {return this.build(Algebra.compile(QueryFactory.create(queryAsString)));}

    public Op build (Op root) {
        for (Var toBind : vars) {
            // Important note: here we have a placeholder because we already have the id, but we
            // don't need the actual value as a string, since we stay in the same engine overall.
            // So this improves (i) performance as we don't need to retrieve the actual value in the database;
            // and (ii) reliability as we get the id from the database, we know for sure it exists as is,
            // while another round of translation would not guarantee it.
            // However, if we happen to output the subquery, it would display placeholders that are meaningless
            // and the query would not return anything. This could be an issue for Sage.

            // this is a URI because it works for all: graph subject predicate object.
            Node valueAsNode = placeholderNode(toBind);
            cache.register(valueAsNode, bindings.get(toBind).getId());
            // right = OpJoin.create(OpExtend.extend(OpTable.unit(), toBind, ExprLib.nodeToExpr(valueAsNode)), right);
        }

        resultVar = Var.alloc(RawerConstants.COUNT_VARIABLE);
        Var subCountVariable = Var.alloc(RawerConstants.COUNT_VARIABLE + "_0.1");

        root = new OpGroup(root, new VarExprList(), List.of(new ExprAggregator(subCountVariable, new AggCount())));
        root = OpExtend.create(root, new VarExprList(resultVar, new ExprVar(subCountVariable)));
        root = new OpProject(root, List.of(resultVar));

        return ReturningOpVisitorRouter.visit(this, root);
    }

    @Override
    public Op visit(OpTriple triple) {
        return new OpTriple(injectPlaceholders(triple.getTriple()));
    }

    @Override
    public Op visit(OpBGP bgp) {
        List<Triple> triples = bgp.getPattern().getList().stream().map(this::injectPlaceholders).toList();
        return new OpBGP(BasicPattern.wrap(triples));
    }

    /**
     * Replace targeted variables by their placeholders in the triple.
     * @param triple The triple without placeholders.
     * @return A new triple with placeholders.
     */
    public Triple injectPlaceholders (Triple triple) {
        return Triple.create(
                (triple.getSubject().isVariable() && vars.contains((Var) triple.getSubject())) ?
                        placeholderNode((Var) triple.getSubject()):
                        triple.getSubject(),
                (triple.getPredicate().isVariable() && vars.contains((Var) triple.getPredicate())) ?
                        placeholderNode((Var) triple.getPredicate()):
                        triple.getPredicate(),
                (triple.getObject().isVariable() && vars.contains((Var) triple.getObject())) ?
                        placeholderNode((Var) triple.getObject()):
                        triple.getObject()
        );
    }

    public static Node placeholderNode(Var var) {
        return NodeFactory.createURI("https://PLACEHOLDER_" + var.getVarName());
    }
}
