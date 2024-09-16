package fr.gdd.raw.accumulators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.jena.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.generics.CacheId;
import fr.gdd.passage.commons.generics.Substitutor;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawConstants;
import fr.gdd.raw.iterators.RandomScan;
import fr.gdd.raw.subqueries.CountSubqueryBuilder;
import org.apache.jena.atlas.lib.tuple.Tuple3;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.util.VarUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * We found a binding µ. Therefore, we know for sure that there exists a result
 * on the COUNT subquery generated, despite the subquery returning 0 because it failed
 * to find a successful random walk.
 * We could then put Fµ = 1, but this would state that such result is unique while
 * it may be a matter of wrong budget.
 * Instead, we want to inject progressively the mappings in the plan, so we can process
 * a Wander Join based on this specific successful random walk.
 * This aims at avoiding zero-knowledge issues.
 */
public class FmuBootstrapper<ID,VALUE> extends ReturningArgsOpVisitor<Double, Set<Var>> {

    final Backend<ID,VALUE,?> backend;
    final CacheId<ID,VALUE> cache;
    final BackendBindings<ID,VALUE> bindings;

    CacheId<ID,VALUE> dedicatedCache;

    public FmuBootstrapper(Backend<ID,VALUE,?> backend, CacheId<ID,VALUE> cache, BackendBindings<ID,VALUE> bindings) {
        this.backend = backend;
        this.cache = cache;
        this.bindings = bindings;

        this.dedicatedCache = new CacheId<>(backend).copy(this.cache);
        for (Var toBind : bindings.vars()) { // all mappings are cached
            // take a look at CountSubqueryBuilder comment to understand why we do this.
            // (tldr: to work on ID, not on String)
            Node valueAsNode = CountSubqueryBuilder.placeholderNode(toBind);
            dedicatedCache.register(valueAsNode, bindings.get(toBind).getId());
        }
    }

    public double visit(String opAsString) {return this.visit(Algebra.compile(QueryFactory.create(opAsString)));}
    public double visit(Op op) {return ReturningArgsOpVisitorRouter.visit(this, op, new HashSet<>());}

    @Override
    public Double visit(OpTriple triple, Set<Var> alreadySetVariables) {
        // not meant to be executed anyway TODO cleaner way to create default context.
        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(RawConstants.BACKEND, backend);
        ec.getContext().set(RawConstants.CACHE, dedicatedCache);

        Triple newTriple = injectBoundVariables(triple.getTriple(), alreadySetVariables);

        Tuple3<ID> spo = Substitutor.substitute(newTriple, new BackendBindings<>(), dedicatedCache);

        RandomScan<ID,VALUE> scan = new RandomScan<>(ec, new OpTriple(newTriple), spo);

        return 1. / scan.cardinality();
    }

    @Override
    public Double visit(OpJoin join, Set<Var> alreadySetVariables) {
        Double leftProbability = ReturningArgsOpVisitorRouter.visit(this, join.getLeft(), alreadySetVariables);
        alreadySetVariables.addAll(OpVars.visibleVars(join.getLeft()));
        Double rightProbability = ReturningArgsOpVisitorRouter.visit(this, join.getRight(), alreadySetVariables);
        return leftProbability * rightProbability;
    }

    @Override
    public Double visit(OpBGP bgp, Set<Var> alreadySetVariables) {
        List<Triple> triples = bgp.getPattern().getList();

        Double probability = 1.;
        for (Triple triple : triples) {
            OpTriple asOpTriple = new OpTriple(triple);
            probability *= this.visit(asOpTriple, alreadySetVariables);
            alreadySetVariables.addAll(VarUtils.getVars(triple));
        }

        return probability;
    }

    @Override
    public Double visit(OpProject project, Set<Var> alreadySetVariables) {
        return ReturningArgsOpVisitorRouter.visit(this, project.getSubOp(), alreadySetVariables);
    }

    @Override
    public Double visit(OpExtend extend, Set<Var> alreadySetVariables) {
        alreadySetVariables.addAll(extend.getVarExprList().getVars());
        return ReturningArgsOpVisitorRouter.visit(this, extend.getSubOp(), alreadySetVariables);
    }

    @Override
    public Double visit(OpTable table, Set<Var> alreadySetVariables) {
        if (table.isJoinIdentity()) {
            return 1.;
        }
        throw new UnsupportedOperationException("Tables are not handled properly yet…");
    }

    @Override
    public Double visit(OpGroup groupBy, Set<Var> alreadySetVariables) {
        return ReturningArgsOpVisitorRouter.visit(this, groupBy.getSubOp(), alreadySetVariables);
    }

    /* *********************************************************************** */

    /**
     * It injects the actual ID when the variable are set.
     * @param t The triple pattern to modify.
     * @param alreadySetVariables The variable that are set before encountering this triple pattern.
     * @return The modified triple pattern.
     */
    public Triple injectBoundVariables(Triple t, Set<Var> alreadySetVariables) {
        Node s = t.getSubject().isVariable() && alreadySetVariables.contains((Var) t.getSubject()) ?
                CountSubqueryBuilder.placeholderNode((Var) t.getSubject()): // placeholder will put the ID in place
                t.getSubject(); // otherwise, the term or variable stays itself
        Node p = t.getPredicate().isVariable() && alreadySetVariables.contains((Var) t.getPredicate()) ?
                CountSubqueryBuilder.placeholderNode((Var) t.getPredicate()):
                t.getPredicate();
        Node o = t.getObject().isVariable() && alreadySetVariables.contains((Var) t.getObject()) ?
                CountSubqueryBuilder.placeholderNode((Var) t.getObject()):
                t.getObject();

        return Triple.create(s, p, o);
    }

}
