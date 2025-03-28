package fr.gdd.passage.volcano.optimizers;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.sparql.core.Var;
import se.liu.ida.hefquin.base.query.SPARQLQuery;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.CardinalityEstimation;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Provides estimation of cardinality based on a backend, and an HeFQUIN physical plan.
 * The backend must be able to process COUNT efficiently.
 */
public class BackendCardinalityEstimation implements CardinalityEstimation {

    final Backend<?,?> backend;

    public BackendCardinalityEstimation(Backend<?,?> backend) {
        this.backend = backend;
    }

    @Override
    public CompletableFuture<Integer> initiateCardinalityEstimation(PhysicalPlan plan) {
        // TODO create an engine that performs cardinality estimations, but for now it will be passage
        return CompletableFuture.supplyAsync(() -> {
            // final Date start = new Date();
            // should also get a COST out of random walks. which corresponds to the difficulty,
            // i.e., the space to explore which also includes failures on the path.
            final int cardinality = (int)(Math.random()*50000); // TODO actually execute the request

            RawOpExecutor executor = new RawOpExecutor();
            executor.setBackend(backend);
            executor.setTimeout(500L);
            // TODO remove the toOp -> toQuery -> toOp
            String asCOUNT = String.format ( "SELECT (COUNT(*) AS ?c) WHERE {%s}",
                    ((SPARQLQuery) plan.getRootOperator()).asJenaQuery().toString());

            Iterator<BackendBindings> it = executor.execute(asCOUNT);

            // only one result
            var count = it.next().get(Var.alloc("c"));

            // final Date stop = new Date();
            // if none, the cardinality should be high
            return cardinality;
        });
    }
}
