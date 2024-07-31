package fr.gdd.sage.sager.optimizers;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.pause.Triples2BGP;
import fr.gdd.sage.sager.resume.BGP2Triples;
import fr.gdd.sage.sager.resume.Subqueries2LeftOfJoins;
import org.apache.jena.sparql.algebra.Op;

/**
 * Create the plan that will be used by the Executor afterward.
 */
public class SagerOptimizer<ID,VALUE> {

    final Backend<ID,VALUE,?> backend;
    final CacheId<ID,VALUE> cache;
    boolean forceOrder = false;

    public SagerOptimizer(Backend<ID,VALUE,?> backend, CacheId<ID,VALUE> cache) {
        this.backend = backend;
        this.cache = cache;
    }

    public Op optimize(Op toOptimize) {
        if (!forceOrder) {
            // for now, it's cardinality based only. TODO register them in lists
            toOptimize = ReturningOpVisitorRouter.visit(new Triples2BGP(), toOptimize);
            toOptimize = new CardinalityJoinOrdering<>(backend, cache).visit(toOptimize); // need to have bgp to optimize, no tps
        }

        toOptimize = ReturningOpVisitorRouter.visit(new BGP2Triples(), toOptimize);
        toOptimize = ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), toOptimize);
        return toOptimize;
    }

    public SagerOptimizer<ID,VALUE> forceOrder() {
        this.forceOrder = true;
        return this;
    }

}
