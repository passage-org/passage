package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.subqueries.CountSubqueryBuilder;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.Var;
import org.checkerframework.checker.units.qual.C;

import java.util.List;
import java.util.Set;

/**
 * We found a binding µ. Therefore, we know for sure that there exists a result
 * on the COUNT subquery generated. We want to inject progressively the mappings
 * in the plan, so we can process a Wander Join based on this specific successful
 * random walk.
 * This aims at avoiding zero-knowledge issues.
 * TODO TODO TODO TODO
 */
public class FmuBootstrapper<ID,VALUE> extends ReturningArgsOpVisitor<Double, Set<Var>> {

    final Backend<ID,VALUE,?> backend;
    final CacheId<ID,VALUE> cache;
    final Set<Var> vars;
    final BackendBindings<ID,VALUE> bindings;

    CacheId<ID,VALUE> dedicatedCache;

    public FmuBootstrapper(Backend<ID,VALUE,?> backend, CacheId<ID,VALUE> cache, BackendBindings<ID,VALUE> bindings, Set<Var> vars) {
        this.backend = backend;
        this.cache = cache;
        this.vars = vars;
        this.bindings = bindings;

        this.dedicatedCache = new CacheId<>(backend).copy(this.cache);
        for (Var toBind : bindings.vars()) { // all mappings are cached
            // take a look at CountSubqueryBuilder comment to understand why we do this.
            // (tldr; to work on ID, not on String)
            Node valueAsNode = CountSubqueryBuilder.placeholderNode(toBind);
            dedicatedCache.register(valueAsNode, bindings.get(toBind).getId());
        }
    }

    @Override
    public Double visit(OpBGP bgp, Set<Var> alreadySetVariables) {
        List<Triple> triples = bgp.getPattern().getList();

        for (int i=0; i < triples.size(); ++i) {


        }


        return 0.; // TODO
    }

    public Triple injectBoundVariables(Triple t, Set<Var> alreadySetVariables) {
       /* Node s = t.getSubject().isVariable() && alreadySetVariables.contains((Var) t.getSubject()) ?
                bindings.get((Var) t.getSubject()):
                t.getSubject(); */

        throw new UnsupportedOperationException("TODO"); //TODO
    }

}
