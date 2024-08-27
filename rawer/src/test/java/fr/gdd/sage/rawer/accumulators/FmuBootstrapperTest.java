package fr.gdd.sage.rawer.accumulators;

import com.bigdata.rdf.sail.BigdataSail;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.rawer.subqueries.CountSubqueryBuilder;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
class FmuBootstrapperTest {

    private static final Logger log = LoggerFactory.getLogger(FmuBootstrapperTest.class);
    private static final BigdataSail blazegraph = IM4Blazegraph.triples9();

    @Disabled
    @Test
    public void on_a_single_triple_pattern () throws RepositoryException {
        final Backend backend = new BlazegraphBackend(blazegraph);

        final RawerOpExecutor executor = new RawerOpExecutor().setBackend(backend);
        final String queryAsString = "SELECT * WHERE {?s <http://address> ?o}";

        Iterator<BackendBindings> iterator = executor.execute(queryAsString);
        assertTrue(iterator.hasNext());
        BackendBindings binding = iterator.next();
        log.debug("Random binding µ: {}", binding);

        FmuBootstrapper bootsrapper = new FmuBootstrapper<>(backend, executor.getCache(), binding);
        // Since no variable is being targeted, it should return the proba of getting an element
        // i.e 1/3 since it has 3 results
        double probability = bootsrapper.visit(queryAsString);
        log.debug("Probability Fµ: {}", probability);
        assertEquals(1./3., probability);
    }

    @Disabled
    @Test
    public void on_a_single_triple_pattern_with_variable_set () throws RepositoryException {
        // variable set meaning that its hardbound beforehand. It does not account
        // as a variable when examining the probability.
        final Backend backend = new BlazegraphBackend(blazegraph);

        final RawerOpExecutor executor = new RawerOpExecutor().setBackend(backend);
        final String queryAsString = "SELECT * WHERE {?s <http://address> ?o}";

        Iterator<BackendBindings> iterator = executor.execute(queryAsString);
        assertTrue(iterator.hasNext());
        BackendBindings binding = iterator.next();
        log.debug("Random binding µ: {}", binding);

        CountSubqueryBuilder subqueryBuilder = new CountSubqueryBuilder<>(backend, binding, Set.of(Var.alloc("s")));
        Op countQuery = subqueryBuilder.build(Algebra.compile(QueryFactory.create(queryAsString)));
        log.debug("Fmu query: {}", OpAsQuery.asQuery(countQuery));

        FmuBootstrapper bootsrapper = new FmuBootstrapper<>(backend, executor.getCache(), binding);
        // Every ?s has only one corresponding ?o, so it's a 100% chance to get it once ?s is set
        double probability = bootsrapper.visit(countQuery);
        log.debug("Probability Fµ: {}", probability);
        assertEquals(1., probability);
    }

    @Disabled
    @Test
    public void single_tp_with_variable_set_but_different_values_for_it () throws RepositoryException {
        final Long LIMIT = 10L;
        final Backend backend = new BlazegraphBackend(blazegraph);

        final RawerOpExecutor executor = new RawerOpExecutor().setBackend(backend).setLimit(LIMIT);
        String queryAsString = "SELECT * WHERE {?s <http://address> ?o}";

        Iterator<BackendBindings> iterator = executor.execute(queryAsString);
        long nbResults = 0L;
        while (iterator.hasNext()) {
            BackendBindings binding = iterator.next();
            nbResults += 1;
            log.debug("Random binding µ: {}", binding);

            CountSubqueryBuilder subqueryBuilder = new CountSubqueryBuilder<>(backend, binding, Set.of(Var.alloc("o")));
            Op countQuery = subqueryBuilder.build(Algebra.compile(QueryFactory.create(queryAsString)));
            log.debug("Fmu query: {}", OpAsQuery.asQuery(countQuery));

            FmuBootstrapper bootsrapper = new FmuBootstrapper<>(backend, executor.getCache(), binding);
            // Every ?s has only one corresponding ?o, so it's a 100% chance to get it once ?s is set
            double probability = bootsrapper.visit(countQuery);
            log.debug("Probability Fµ: {}", probability);
            if (binding.get(Var.alloc("o")).getString().contains("nantes")) {
                assertEquals(1./2., probability);
            } else if (binding.get(Var.alloc("o")).getString().contains("paris")) {
                assertEquals(1., probability);
            }
        }
        assertEquals(LIMIT, nbResults);
    }

    @Disabled
    @Test
    public void two_tps_with_variable_set () throws RepositoryException {
        final long LIMIT_SCANS = 50L;
        final Backend backend = new BlazegraphBackend(blazegraph);

        RawerOpExecutor executor = new RawerOpExecutor().setBackend(backend).setLimit(LIMIT_SCANS);
        String queryAsString = "SELECT * WHERE {?s <http://address> ?c . ?s <http://own> ?a}";
        Var examinedVariable = Var.alloc("s");

        Iterator<BackendBindings> iterator = executor.execute(queryAsString);
        while (iterator.hasNext()) {
            BackendBindings binding = iterator.next();
            log.debug("Random binding µ: {}", binding);

            CountSubqueryBuilder subqueryBuilder = new CountSubqueryBuilder<>(backend, binding, Set.of(examinedVariable));
            Op countQuery = subqueryBuilder.build(Algebra.compile(QueryFactory.create(queryAsString)));
            log.debug("Fmu query: {}", OpAsQuery.asQuery(countQuery));

            FmuBootstrapper bootsrapper = new FmuBootstrapper<>(backend, executor.getCache(), binding);
            double probability = bootsrapper.visit(countQuery);
            log.debug("Probability Fµ: {}", probability);
            assertEquals(1./3., probability); // cat and dog and snake had all (1/3)*(1/1) chances to exist
        }
    }


}