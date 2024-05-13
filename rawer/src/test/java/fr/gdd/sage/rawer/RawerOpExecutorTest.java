package fr.gdd.sage.rawer;

import com.bigdata.rdf.sail.BigdataSail;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import fr.gdd.sage.databases.inmemory.IM4Jena;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.ext.com.google.common.collect.HashMultiset;
import org.apache.jena.ext.com.google.common.collect.Multiset;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class RawerOpExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(RawerOpExecutorTest.class);
    private static final Dataset dataset = IM4Jena.triple9();
    private static final BigdataSail blazegraph = IM4Blazegraph.triples9();

    @Test
    public void select_all_from_simple_spo () { // as per usual
        String queryAsString = "SELECT * WHERE {?s ?p ?o}";
        Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        assertEquals(9, results.elementSet().size());

        Multiset<String> resultsBlaze = execute(queryAsString, new BlazegraphBackend(blazegraph), 1000L);
        // spo contains other default triples… That why we need more than 100L to
        // retrieve expected spo with high probability.
        results.elementSet().forEach(e -> assertTrue(resultsBlaze.contains(e)));
    }

    @Test
    public void simple_project_on_spo () { // as per usual
        String queryAsString = "SELECT ?s WHERE {?s ?p ?o}";
        Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        assertEquals(6, results.elementSet().size()); // Alice repeated 4 times

        Multiset<String> resultsBlaze = execute(queryAsString, new BlazegraphBackend(blazegraph), 1000L);
        // spo contains other default triples… That why we need more than 100L to
        // retrieve expected spo with high probability.
        results.elementSet().forEach(e -> assertTrue(resultsBlaze.contains(e)));
    }

    @Test
    public void simple_triple_pattern () {
        String queryAsString = "SELECT * WHERE {?s <http://address> ?o}";
        Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        assertEquals(3, results.elementSet().size());

        Multiset<String> resultsBlaze = execute(queryAsString, new BlazegraphBackend(blazegraph), 1000L);
        // spo contains other default triples… That why we need more than 100L to
        // retrieve expected spo with high probability.
        results.elementSet().forEach(e -> assertTrue(resultsBlaze.contains(e)));
        assertEquals(3, resultsBlaze.elementSet().size());
    }

    @Test
    public void simple_bgp() {
        String queryAsString = "SELECT * WHERE {?s <http://address> ?c . ?s <http://own> ?a}";
        Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        assertEquals(3, results.elementSet().size());

        Multiset<String> resultsBlaze = execute(queryAsString, new BlazegraphBackend(blazegraph), 1000L);
        // spo contains other default triples… That why we need more than 100L to
        // retrieve expected spo with high probability.
        results.elementSet().forEach(e -> assertTrue(resultsBlaze.contains(e)));
        assertEquals(3, resultsBlaze.elementSet().size());
    }

    @Test
    public void simple_bgp_of_3_tps() {
        String queryAsString = "SELECT * WHERE {?s <http://address> ?c . ?s <http://own> ?a . ?a <http://species> ?r}";
        Multiset<String> results = execute(queryAsString, new JenaBackend(dataset), 100L);
        assertEquals(3, results.elementSet().size());

        Multiset<String> resultsBlaze = execute(queryAsString, new BlazegraphBackend(blazegraph), 1000L);
        // spo contains other default triples… That why we need more than 100L to
        // retrieve expected spo with high probability.
        results.elementSet().forEach(e -> assertTrue(resultsBlaze.contains(e)));
        assertEquals(3, resultsBlaze.elementSet().size());
    }

    @Disabled
    @Test
    public void simple_bind_on_a_triple_pattern () {
        String queryAsString = "SELECT * WHERE {BIND (<http://Alice> AS ?s) ?s ?p ?o}";
        execute(queryAsString, new JenaBackend(dataset), 100L);
    }

    @Disabled
    @Test
    public void count_of_simple_triple_pattern () {
        String queryAsString = "SELECT (COUNT(*) AS ?c) WHERE {?s ?p ?o}";
        execute(queryAsString, new JenaBackend(dataset), 10L);
    }

    @Disabled
    @Test
    public void count_with_group_on_simple_tp () {
        String queryAsString = "SELECT (COUNT(*) AS ?c) ?p WHERE {?s ?p ?o} GROUP BY ?p";
        execute(queryAsString, new JenaBackend(dataset), 10L);
    }

    @Disabled
    @Test
    public void count_distinct_of_simple_triple_pattern () {
        String queryAsString = "SELECT (COUNT(DISTINCT *) AS ?c) WHERE {?s ?p ?o}";
        execute(queryAsString, new JenaBackend(dataset), 10L);
    }

    /* ************************************************************* */

    /**
     * @param queryAsString The query to execute.
     * @param backend The backend to use.
     * @param limit The number of random walks to perform.
     * @return The random solutions mappings.
     */
    public static Multiset<String> execute(String queryAsString, Backend<?,?,?> backend, Long limit) {
        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(RawerConstants.BACKEND, backend);
        ARQ.enableOptimizer(false);
        RawerOpExecutor<?,?> executor = new RawerOpExecutor<NodeId, Node>(ec).setLimit(limit);

        // QueryIterator iterator = executor.execute(query);
        Iterator<? extends BackendBindings<?, ?>> iterator = executor.execute(query);

        Multiset<String> results = HashMultiset.create();
        while (iterator.hasNext()) {
            String binding = iterator.next().toString();
            results.add(binding);
            log.debug("{}", binding);
        }
        assertEquals(limit, results.size());
        return results;
    }

}