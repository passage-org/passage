package fr.gdd.passage.volcano.optimizers;

import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.datatypes.xsd.impl.XSDDouble;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Var;
import se.liu.ida.hefquin.engine.queryplan.physical.PhysicalPlan;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.CardinalityEstimation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Provides estimation of cardinality based on a backend, and an HeFQUIN physical plan.
 * The backend must be able to process COUNT efficiently.
 */
public class BackendCardinalityEstimation implements CardinalityEstimation {

    final Backend<?,?> backend;
    HashMap<String, Integer> cache = new HashMap<>();

    public BackendCardinalityEstimation(Backend<?,?> backend) {
        this.backend = backend;
    }

    @Override
    public CompletableFuture<Integer> initiateCardinalityEstimation(PhysicalPlan plan) {
        return CompletableFuture.supplyAsync(() -> {
            Op physicalPlanAsJena = Jena2HeFQUINLogicalPlans.convert(plan);
            String asCOUNT = String.format ( "SELECT (COUNT(*) AS ?c) WHERE {%n%s}",
                    OpAsQuery.asQuery(physicalPlanAsJena).toString());
            if (cache.containsKey(asCOUNT)) {
                // TODO instead, ask for more random walks
                return cache.get(asCOUNT);
            }

            // final Date start = new Date();
            // should also get a COST out of random walks. which corresponds to the difficulty,
            // i.e., the space to explore which also includes failures on the path.
            final int cardinality = (int)(Math.random()*50000); // TODO actually execute the request

            RawOpExecutor executor = new RawOpExecutor()
                    .setBackend(backend)
                    .setTimeout(5000L);
                    // .setLimit(10000L);

            // TODO remove the toOp -> toQuery -> toOp



            Iterator<BackendBindings> it = executor.execute(asCOUNT);

            if (it.hasNext()) {
                // only one result
                System.out.println();
                System.out.println(asCOUNT);
                var count = it.next().get(Var.alloc("c"));
                System.out.println("Cardinality estimation: " + count);
                int value = switch (count.getLiteralDatatype()) {
                    case XSDDouble ignored -> (int) Double.parseDouble(count.getLiteralLexicalForm());
                    default -> throw new UnsupportedOperationException("Return value of count is not supported: " + count.getLiteralDatatypeURI());
                };
                cache.put(asCOUNT, value);
            }

            // final Date stop = new Date();
            // if none, the cardinality should be high
            return cardinality; // TODO Integer.MAXVALUE
        });
    }
}
