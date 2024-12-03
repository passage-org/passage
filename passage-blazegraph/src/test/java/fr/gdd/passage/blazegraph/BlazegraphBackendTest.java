package fr.gdd.passage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.ISPO;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.commons.iterators.BackendLazyIterator;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class BlazegraphBackendTest {

    private final static Logger log = LoggerFactory.getLogger(BlazegraphBackendTest.class);
    private static final String WATDIV = "/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl";
    public static final BlazegraphBackend watdiv;
    private static final String WDBENCH = "/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl";
    public static final BlazegraphBackend wdbench;

    static {
        try {
            watdiv = Path.of(WATDIV).toFile().exists() ? new BlazegraphBackend(WATDIV) : null;
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        try {
            wdbench = Path.of(WDBENCH).toFile().exists() ? new BlazegraphBackend(WDBENCH) : null;
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }


    /* ************************************************************************* */

    @Test
    public void create_values_with_string_repr () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        BigdataValue twelve = bb.getValue("\"12\"");
        assertInstanceOf(BigdataLiteral.class, twelve);
        assertEquals("\"12\"", twelve.toString());
        BigdataValue uri = bb.getValue("<https://uri>");
        assertInstanceOf(BigdataURI.class, uri);
        assertEquals("<https://uri>", uri.toString());
        bb.close();
    }

    @Test
    public void create_a_simple_pet_dataset () throws QueryEvaluationException, MalformedQueryException, RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

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
    public void creating_simple_iterators () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV own = bb.getId("<http://own>", SPOC.PREDICATE);
        IV species = bb.getId("<http://species>", SPOC.PREDICATE);
        IV nantes = bb.getId("<http://nantes>", SPOC.OBJECT);

        executeSimpleTP(bb, bb.any(), address, bb.any(), 3);
        executeSimpleTP(bb, bb.any(), own, bb.any(), 3);
        executeSimpleTP(bb, bb.any(), species, bb.any(), 3);
        executeSimpleTP(bb, bb.any(), bb.any(), nantes, 2);

        bb.close();
    }

    @Test
    public void creating_simple_random () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        BackendLazyIterator<IV, BigdataValue, Long> li = (BackendLazyIterator<IV, BigdataValue, Long>) bb.search(bb.any(), address, bb.any());

        Multiset<BindingSet> results = HashMultiset.create();
        for (int i = 0; i < 10_000; ++i) {
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
        log.debug(uniques.toString());
        bb.close();
    }

    @Test
    public void skipping_some_elements_from_triple_patterns () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);

        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 0, 3);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 1, 2);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 2, 1);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 3, 0);
        executeSimpleTPWithSkip(bb, bb.any(), address, bb.any(), 18, 0);

        bb.close();
    }

    @Test
    public void getting_cardinalities_from_triple_patterns () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV own = bb.getId("<http://own>", SPOC.PREDICATE);
        IV species = bb.getId("<http://species>", SPOC.PREDICATE);
        IV nantes = bb.getId("<http://nantes>", SPOC.OBJECT);

        assertEquals(3, getCardinality(bb, bb.any(), address, bb.any()));
        assertEquals(3, getCardinality(bb, bb.any(), own, bb.any()));
        assertEquals(3, getCardinality(bb, bb.any(), species, bb.any()));
        assertEquals(2, getCardinality(bb, bb.any(), bb.any(), nantes));

        bb.close();
    }

    @Disabled
    @Test
    public void on_watdiv_conjunctive_with_blazegraph_engine () throws QueryEvaluationException, MalformedQueryException, RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(WATDIV);
        bb.executeQuery("""
                SELECT * WHERE {
                    hint:Query hint:optimizer "None" .
                    ?v0 <http://xmlns.com/foaf/age> <http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup2> .
                    ?v0 <http://schema.org/nationality> ?v8 .
                    ?v2 <http://schema.org/eligibleRegion> ?v8 .
                    ?v2 <http://purl.org/goodrelations/includes> ?v3 .
                }""");
        bb.close();
    }

    @Test
    public void on_watdiv_conjunctive_query_with_compiled_query () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Objects.nonNull(watdiv));

        long start = System.currentTimeMillis();
        final var any = watdiv.any();
        final var p_1 = watdiv.getId("<http://xmlns.com/foaf/age>", SPOC.PREDICATE);
        final var o_1 = watdiv.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup2>", SPOC.OBJECT);
        final var p_2 = watdiv.getId("<http://schema.org/nationality>", SPOC.PREDICATE);
        final var p_3 = watdiv.getId("<http://schema.org/eligibleRegion>", SPOC.PREDICATE);
        final var p_4 = watdiv.getId("<http://purl.org/goodrelations/includes>", SPOC.PREDICATE);

        BackendIterator<IV, BigdataValue, Long> i_1 = watdiv.search(any, p_1, o_1);

        long nbElements = 0;

        while (i_1.hasNext()) {
            i_1.next();
            BackendIterator<IV, BigdataValue, Long> i_2 = watdiv.search(i_1.getId(SPOC.SUBJECT), p_2, any);
            while (i_2.hasNext()) {
                i_2.next();
                BackendIterator<IV, BigdataValue, Long> i_3 = watdiv.search(any, p_3, i_2.getId(SPOC.OBJECT));
                while (i_3.hasNext()) {
                    i_3.next();
                    BackendIterator<IV, BigdataValue, Long> i_4 = watdiv.search(i_3.getId(SPOC.SUBJECT), p_4, any);
                    while (i_4.hasNext()) {
                        i_4.next();
                        nbElements += 1;
                    }
                }
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        log.debug("Nb elements = {}", nbElements);
        assertEquals(11255178, nbElements);
        log.debug("Duration = {} ms", elapsed);
    }

    @Test
    public void on_watdiv_test_some_cardinalities () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Objects.nonNull(watdiv));

        IV eligibleRegion = watdiv.getId("<http://schema.org/eligibleRegion>", SPOC.PREDICATE);
        IV country21 = watdiv.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>", SPOC.OBJECT);
        IV validThrough = watdiv.getId("<http://purl.org/goodrelations/validThrough>", SPOC.PREDICATE);
        IV includes = watdiv.getId("<http://purl.org/goodrelations/includes>", SPOC.PREDICATE);
        IV text = watdiv.getId("<http://schema.org/text>", SPOC.PREDICATE);
        IV eligibleQuantity = watdiv.getId("<http://schema.org/eligibleQuantity>", SPOC.PREDICATE);
        IV price = watdiv.getId("<http://purl.org/goodrelations/price>", SPOC.PREDICATE);

        BackendLazyIterator lit = (BackendLazyIterator) watdiv.search(watdiv.any(), eligibleRegion, country21);
        assertEquals(2613L, lit.cardinality());
        lit = (BackendLazyIterator) watdiv.search(watdiv.any(), validThrough, watdiv.any());
        assertEquals(36346L, lit.cardinality());
        lit = (BackendLazyIterator) watdiv.search(watdiv.any(), includes, watdiv.any());
        assertEquals(90000L, lit.cardinality());
        lit = (BackendLazyIterator) watdiv.search(watdiv.any(), text, watdiv.any());
        assertEquals(7476L, lit.cardinality());
        lit = (BackendLazyIterator) watdiv.search(watdiv.any(), eligibleQuantity, watdiv.any());
        assertEquals(90000L, lit.cardinality());
        lit = (BackendLazyIterator) watdiv.search(watdiv.any(), price, watdiv.any());
        assertEquals(240000L, lit.cardinality());
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
            log.info("{} {} {}", it.getString(SPOC.SUBJECT), it.getString(SPOC.PREDICATE), it.getString(SPOC.OBJECT));
            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("s", it.getId(SPOC.SUBJECT));
            bs.addBinding("p", it.getId(SPOC.PREDICATE));
            bs.addBinding("o", it.getId(SPOC.OBJECT));
            results.add(bs);
        }
        assertEquals(expectedNb, results.size());
        // cardinality should be exact without deletions in btree
        assertEquals(expectedNb, ((BackendLazyIterator<?,?,?>) it).cardinality());
        return results;
    }

    public Multiset<BindingSet> executeSimpleTPWithSkip(BlazegraphBackend bb, IV s, IV p, IV o, long skip, long expectedNb) {
        BackendIterator<IV, BigdataValue, Long> it = bb.search(s, p, o);
        BlazegraphIterator bit = (BlazegraphIterator) ((BackendLazyIterator)it).getWrapped();
        bit.skip(skip);
        Multiset<BindingSet> results = HashMultiset.create();
        while (it.hasNext()) {
            it.next();
            log.info("skipped {}: {} {} {}", skip,
                    it.getString(SPOC.SUBJECT), it.getString(SPOC.PREDICATE), it.getString(SPOC.OBJECT));
            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("s", it.getId(SPOC.SUBJECT));
            bs.addBinding("p", it.getId(SPOC.PREDICATE));
            bs.addBinding("o", it.getId(SPOC.OBJECT));
            results.add(bs);
        }
        assertEquals(expectedNb, results.size());
        assertEquals(expectedNb, Math.max(0, ((BackendLazyIterator<?,?,?>) it).cardinality() - skip));
        return results;
    }

    public long getCardinality(BlazegraphBackend bb, IV s, IV p, IV o) {
        BackendIterator<IV, BigdataValue, Long> it = bb.search(s, p, o);
        return (long) it.cardinality();
    }

}