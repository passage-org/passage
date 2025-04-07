package fr.gdd.passage.volcano.optimizers;

import fr.gdd.passage.commons.generics.BackendCache;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.transforms.BGP2Triples;
import fr.gdd.passage.commons.transforms.Graph2Quads;
import fr.gdd.passage.commons.transforms.Patterns2Quad;
import fr.gdd.passage.commons.transforms.Triples2BGP;
import fr.gdd.passage.volcano.transforms.DistinctQuery2QueryOfDistincts;
import fr.gdd.passage.volcano.transforms.Quad2Patterns;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.algebra.optimize.TransformMergeBGPs;
import org.apache.jena.sparql.algebra.optimize.TransformPattern2Join;
import org.apache.jena.sparql.algebra.optimize.TransformSimplify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create the plan that will be used by the Executor afterward.
 * TODO easy way to register and order visitor and transformer to be applied in configuration.
 */
public class PassageOptimizer<ID,VALUE> {

    private static final Logger log = LoggerFactory.getLogger(PassageOptimizer.class);

    final Backend<ID,VALUE> backend;
    final BackendCache<ID,VALUE> cache;
    boolean forceOrder = false;

    public PassageOptimizer(Backend<ID,VALUE> backend, BackendCache<ID,VALUE> cache) {
        this.backend = backend;
        this.cache = cache;
    }

    public Op optimize(Op toOptimize) {
        toOptimize = Transformer.transform(new TransformFilterPlacement(), toOptimize);

        toOptimize = new Graph2Quads().visit(toOptimize);
        if (!forceOrder) {
            toOptimize = new Triples2BGP().visit(toOptimize);
            toOptimize = Transformer.transform(new TransformMergeBGPs(), toOptimize);
            toOptimize = new Quad2Patterns().visit(toOptimize);
            // for now, it's cardinality based only. TODO register them in lists
            toOptimize = new CardinalityJoinOrdering<>(backend, cache).visit(toOptimize); // need to have bgp to optimize, no tps
        }

        toOptimize = new DistinctQuery2QueryOfDistincts().visit(toOptimize);
        toOptimize = new Patterns2Quad().visit(toOptimize);
        toOptimize = new BGP2Triples().visit(toOptimize);
        toOptimize = Transformer.transform(new TransformPattern2Join(), toOptimize);
        toOptimize = Transformer.transform(new TransformSimplify(), toOptimize);
        // toOptimize = ReturningOpVisitorRouter.visit(new Subqueries2LeftOfJoins(), toOptimize);

        try {
            log.debug("Optimized: {}", OpAsQuery.asQuery(toOptimize).toString());
        } catch (Exception e) {
            // nothing but:
            // TODO OpQuad are not handled, but OpQuadPatterns are, so we should replace the former by the latter
        }
        return toOptimize;
    }

    public PassageOptimizer<ID,VALUE> forceOrder() {
        this.forceOrder = true;
        return this;
    }

}
