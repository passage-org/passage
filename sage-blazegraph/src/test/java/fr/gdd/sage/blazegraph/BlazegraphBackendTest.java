package fr.gdd.sage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
class BlazegraphBackendTest {

    private final static Logger log = LoggerFactory.getLogger(BlazegraphBackendTest.class);

    @Test
    public void create_a_simple_pet_dataset () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend(SmallDatasetsForTests.getPetsDataset());

        // There is nothing but the default ~30ish triples inside.
        Multiset<BindingSet> results = bb.executeQuery("SELECT * WHERE {?s <http://address> ?o}");
        assertEquals(3, results.size());

        results = bb.executeQuery("SELECT * WHERE {?s <http://own> ?o}");
        assertEquals(3, results.size());

        results = bb.executeQuery("SELECT * WHERE {?s <http://species> ?o}");
        assertEquals(3, results.size());

        results = bb.executeQuery("SELECT * WHERE {?s ?p <http://nantes>}");
        assertEquals(2, results.size());

        bb.close();
    }

    @Test
    public void creating_simple_iterators () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend(SmallDatasetsForTests.getPetsDataset());

        IV address = bb.getId("http://address", SPOC.PREDICATE);
        IV own = bb.getId("http://own", SPOC.PREDICATE);
        IV species = bb.getId("http://species", SPOC.PREDICATE);
        IV nantes = bb.getId("http://nantes", SPOC.OBJECT);

        Multiset<BindingSet> results = executeSimpleTP(bb, bb.any(), address, bb.any(), 3);
        results = executeSimpleTP(bb, bb.any(), own, bb.any(), 3);
        results = executeSimpleTP(bb, bb.any(), species, bb.any(), 3);
        results = executeSimpleTP(bb, bb.any(), bb.any(), nantes, 2);

        bb.close();
    }

    @Test
    public void creating_simple_random () {
        BlazegraphBackend bb = new BlazegraphBackend(SmallDatasetsForTests.getPetsDataset());
        IV address = bb.getId("http://address", SPOC.PREDICATE);
        LazyIterator<IV, BigdataValue, Long> li = (LazyIterator<IV, BigdataValue, Long>) bb.search(bb.any(), address, bb.any());

        Multiset<BindingSet> results = HashMultiset.create();
        for (int i = 0; i < 10000; ++i) {
            ISPO spo = ((BlazegraphIterator) li.getWrapped()).getUniformRandomSPO();
            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("s", spo.s());
            bs.addBinding("p", spo.p());
            bs.addBinding("o", spo.o());
            results.add(bs);
        }

        Set<Multiset.Entry<BindingSet>> uniques = results.entrySet();

        assertEquals(10_000, results.size());
        assertEquals(3, uniques.size());
        for (var entry : uniques) {
            assertEquals(address, entry.getElement().getValue("p"));
        }
    }

    @Test
    public void testing_some_skipperino () {
        BlazegraphBackend bb = new BlazegraphBackend(SmallDatasetsForTests.getPetsDataset());
        IV address = bb.getId("http://address", SPOC.PREDICATE);

        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 0, 3);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 1, 2);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 2, 1);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 3, 0);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 18, 0);
    }

    @Disabled
    @Test
    public void on_watdiv_conjunctive_with_blazegraph_engine () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");
        bb.executeQuery("""
                SELECT * WHERE {
                    hint:Query hint:optimizer "None" .
                    ?v0 <http://xmlns.com/foaf/age> <http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup2> .
                    ?v0 <http://schema.org/nationality> ?v8 .
                    ?v2 <http://schema.org/eligibleRegion> ?v8 .
                    ?v2 <http://purl.org/goodrelations/includes> ?v3 .
                }""");
    }

    @Disabled
    @Test
    public void on_watdiv_conjunctive_query_with_compiled_query () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

        long start = System.currentTimeMillis();
        final var any = bb.any();
        final var p_1 = bb.getId("http://xmlns.com/foaf/age", SPOC.PREDICATE);
        final var o_1 = bb.getId("http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup2", SPOC.OBJECT);
        final var p_2 = bb.getId("http://schema.org/nationality", SPOC.PREDICATE);
        final var p_3 = bb.getId("http://schema.org/eligibleRegion", SPOC.PREDICATE);
        final var p_4 = bb.getId("http://purl.org/goodrelations/includes", SPOC.PREDICATE);

        BackendIterator<IV, BigdataValue, Long> i_1 = bb.search(any, p_1, o_1);

        long nbElements = 0;

        while (i_1.hasNext()) {
            i_1.next();
            BackendIterator<IV, BigdataValue, Long> i_2 = bb.search(i_1.getId(SPOC.SUBJECT), p_2, any);
            while (i_2.hasNext()) {
                i_2.next();
                BackendIterator<IV, BigdataValue, Long> i_3 = bb.search(any, p_3, i_2.getId(SPOC.OBJECT));
                while (i_3.hasNext()) {
                    i_3.next();
                    BackendIterator<IV, BigdataValue, Long> i_4 = bb.search(i_3.getId(SPOC.SUBJECT), p_4, any);
                    while (i_4.hasNext()) {
                        i_4.next();
                        nbElements += 1;
                    }
                }
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        log.debug("Nb elements = {}", nbElements);
        log.debug("Duration = {} ms", elapsed);
    }

    @Disabled
    @Test
    public void on_watdiv_test_some_cardinalities () {
        BlazegraphBackend bb = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

        IV eligibleRegion = bb.getId("http://schema.org/eligibleRegion", SPOC.PREDICATE);
        IV country21 = bb.getId("http://db.uwaterloo.ca/~galuc/wsdbm/Country21", SPOC.OBJECT);
        IV validThrough = bb.getId("http://purl.org/goodrelations/validThrough", SPOC.PREDICATE);
        IV includes = bb.getId("http://purl.org/goodrelations/includes", SPOC.PREDICATE);
        IV text = bb.getId("http://schema.org/text", SPOC.PREDICATE);
        IV eligibleQuantity = bb.getId("http://schema.org/eligibleQuantity", SPOC.PREDICATE);
        IV price = bb.getId("http://purl.org/goodrelations/price", SPOC.PREDICATE);

        LazyIterator lit = (LazyIterator) bb.search(bb.any(), eligibleRegion, country21);
        assertEquals(2613L, lit.cardinality());
        lit = (LazyIterator) bb.search(bb.any(), validThrough, bb.any());
        assertEquals(36346L, lit.cardinality());
        lit = (LazyIterator) bb.search(bb.any(), includes, bb.any());
        assertEquals(90000L, lit.cardinality());
        lit = (LazyIterator) bb.search(bb.any(), text, bb.any());
        assertEquals(7476L, lit.cardinality());
        lit = (LazyIterator) bb.search(bb.any(), eligibleQuantity, bb.any());
        assertEquals(90000L, lit.cardinality());
        lit = (LazyIterator) bb.search(bb.any(), price, bb.any());
        assertEquals(240000L, lit.cardinality());

        bb.close();
    }

    /* ***************************************************************** */

    /**
     * Tests both the number of elements returned by the iterator, and the cardinality
     * estimate provided. The latter should be exact as no deletion has been performed
     * in the augmented btree.
     */
    public Multiset<BindingSet> executeSimpleTP(BlazegraphBackend bb, IV s, IV p, IV o, long expectedNb) {
        BackendIterator<IV, BigdataValue, Long> it = bb.search(s, p, o);
        Multiset<BindingSet> results = HashMultiset.create();
        while (it.hasNext()) {
            it.next();
            log.info("{} {} {}", it.getValue(SPOC.SUBJECT), it.getValue(SPOC.PREDICATE), it.getValue(SPOC.OBJECT));
            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("s", it.getId(SPOC.SUBJECT));
            bs.addBinding("p", it.getId(SPOC.PREDICATE));
            bs.addBinding("o", it.getId(SPOC.OBJECT));
            results.add(bs);
        }
        assertEquals(expectedNb, results.size());
        // cardinality should be exact without deletions in btree
        assertEquals(expectedNb, ((LazyIterator<?,?,?>) it).cardinality());
        return results;
    }

    public Multiset<BindingSet> executeSimpleTPWithSkip(BlazegraphBackend bb, IV s, IV p, IV o, long skip, long expectedNb) {
        BackendIterator<IV, BigdataValue, Long> it = bb.search(s, p, o);
        BlazegraphIterator bit = (BlazegraphIterator) ((LazyIterator)it).getWrapped();
        bit.skip(skip);
        Multiset<BindingSet> results = HashMultiset.create();
        while (it.hasNext()) {
            it.next();
            log.info("skipped {}: {} {} {}", skip,
                    it.getValue(SPOC.SUBJECT), it.getValue(SPOC.PREDICATE), it.getValue(SPOC.OBJECT));
            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("s", it.getId(SPOC.SUBJECT));
            bs.addBinding("p", it.getId(SPOC.PREDICATE));
            bs.addBinding("o", it.getId(SPOC.OBJECT));
            results.add(bs);
        }
        assertEquals(expectedNb, results.size());
        assertEquals(expectedNb, Math.max(0, ((LazyIterator<?,?,?>) it).cardinality() - skip));
        return results;
    }

}