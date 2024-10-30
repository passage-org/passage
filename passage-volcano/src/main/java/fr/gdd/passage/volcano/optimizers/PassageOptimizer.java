package fr.gdd.passage.volcano.optimizers;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.pause.Triples2BGP;
import fr.gdd.passage.volcano.resume.BGP2Triples;
import fr.gdd.passage.volcano.resume.Subqueries2LeftOfJoins;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;

/**
 * Create the plan that will be used by the Executor afterward.
 */
public class PassageOptimizer<ID,VALUE> {

    final Backend<ID,VALUE,Long> backend;
    final BackendCache<ID,VALUE> cache;
    boolean forceOrder = false;

    public PassageOptimizer(Backend<ID,VALUE,Long> backend, BackendCache<ID,VALUE> cache) {
        this.backend = backend;
        this.cache = cache;
    }

    public Op optimize(Op toOptimize) {
        toOptimize = Transformer.transform(new TransformFilterPlacement(), toOptimize);

        if (!forceOrder) {
            toOptimize = ReturningOpVisitorRouter.visit(new Triples2BGP(), toOptimize);
            // for now, it's cardinality based only. TODO register them in lists
            toOptimize = new CardinalityJoinOrdering<>(backend, cache).visit(toOptimize); // need to have bgp to optimize, no tps
        }

        toOptimize = ReturningOpVisitorRouter.visit(new BGP2Triples(), toOptimize);
        toOptimize = ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), toOptimize);
        return toOptimize;
    }

    public PassageOptimizer<ID,VALUE> forceOrder() {
        this.forceOrder = true;
        return this;
    }

}
