package fr.gdd.passage.volcano.optimizers;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.transforms.Triples2BGP;
import fr.gdd.passage.volcano.transforms.BGP2Triples;
import fr.gdd.passage.volcano.transforms.Graph2Quads;
import fr.gdd.passage.volcano.transforms.Patterns2Quad;
import fr.gdd.passage.volcano.transforms.Quad2Patterns;
import org.apache.jena.sparql.algebra.*;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.algebra.optimize.TransformMergeBGPs;
import org.apache.jena.sparql.algebra.optimize.TransformPattern2Join;
import org.apache.jena.sparql.algebra.optimize.TransformSimplify;

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

        toOptimize = ReturningOpVisitorRouter.visit(new Graph2Quads(), toOptimize);
        if (!forceOrder) {
            toOptimize = ReturningOpVisitorRouter.visit(new Triples2BGP(), toOptimize);
            toOptimize = Transformer.transform(new TransformMergeBGPs(), toOptimize);
            toOptimize = ReturningOpVisitorRouter.visit(new Quad2Patterns(), toOptimize);
            // for now, it's cardinality based only. TODO register them in lists
            toOptimize = new CardinalityJoinOrdering<>(backend, cache).visit(toOptimize); // need to have bgp to optimize, no tps
        }

        toOptimize = ReturningOpVisitorRouter.visit(new Patterns2Quad(), toOptimize);
        toOptimize = ReturningOpVisitorRouter.visit(new BGP2Triples(), toOptimize);
        toOptimize = Transformer.transform(new TransformPattern2Join(), toOptimize);
        toOptimize = Transformer.transform(new TransformSimplify(), toOptimize);
        // toOptimize = ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), toOptimize);
        return toOptimize;
    }

    public PassageOptimizer<ID,VALUE> forceOrder() {
        this.forceOrder = true;
        return this;
    }

}
