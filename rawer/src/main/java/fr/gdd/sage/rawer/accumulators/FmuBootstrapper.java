package fr.gdd.sage.rawer.accumulators;

import fr.gdd.jena.visitors.ReturningArgsOpVisitor;
import fr.gdd.sage.generics.CacheId;
import fr.gdd.sage.interfaces.Backend;
import org.apache.jena.sparql.core.Var;

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
public class FmuBootstrapper<ID,VALUE> extends ReturningArgsOpVisitor<Double, List<Var>> {

    final Backend<ID,VALUE,?> backend;
    final CacheId<ID,VALUE> cache;
    final Set<Var> vars;

    public FmuBootstrapper(Backend<ID, VALUE, ?> backend, CacheId<ID,VALUE> cache, Set<Var> vars) {
        this.backend = backend;
        this.cache = cache;
        this.vars = vars;
    }


}
