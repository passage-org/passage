package fr.gdd.passage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


public class BlazegraphDistinctIteratorDXVTest {

    private static final Logger log = LoggerFactory.getLogger(BlazegraphDistinctIteratorDXVTest.class);

    @Test
    public void get_distinct_s_over_a_simple_triple_pattern() throws RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BlazegraphDistinctIteratorDXV it = new BlazegraphDistinctIteratorDXV(bb.store,
                any, address, any, any,
                Set.of(SPOC.SUBJECT)); // distinct (?s)

        int nbResults = 0;
        while (it.hasNext()) {
            it.next();
            ++nbResults;
            log.debug("{} {}", nbResults, it.getString(SPOC.SUBJECT));
        }
        assertEquals(3, nbResults); // Alice, Bob, and Carol

        bb.close();
    }

//    @Test
//    public void throws_when_trying_to_get_access_to_not_distinct() throws RepositoryException {
//        BlazegraphBackend bb = new BlazegraphBackend(IM4Blazegraph.triples9());
//
//        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
//        IV any = bb.any();
//
//        BlazegraphDistinctIterator it = new BlazegraphDistinctIterator(bb.store,
//                any, address, any, any,
//                Set.of(SPOC.OBJECT)); // distinct (?o)
//
//        int nbResults = 0;
//        while (it.hasNext()) {
//            it.next();
//            ++nbResults;
//            assertThrows(UndefinedCode.class, () -> it.getString(SPOC.SUBJECT)); // try access to subject while distinct on objects
//        }
//        assertEquals(2, nbResults); // nantes and paris
//
//        bb.close();
//    }
//
//
    @Test
    public void retrieve_the_offset_of_simple_triple_pattern () throws RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BlazegraphDistinctIteratorDXV it = new BlazegraphDistinctIteratorDXV(bb.store,
                any, address, any, any,
                Set.of(SPOC.SUBJECT));

        long nbResults = 0;
        while (it.hasNext()) {
            it.next();
            ++nbResults;
            log.debug("offset {} for {}", it.current(), it.getString(SPOC.SUBJECT));
        }
        // [main] DEBUG BlazegraphDistinctIteratorTest - offset 3 for <http://Alice>
        // [main] DEBUG BlazegraphDistinctIteratorTest - offset 7 for <http://Bob>
        // [main] DEBUG BlazegraphDistinctIteratorTest - offset 8 for <http://Carol>
        assertEquals(3, nbResults); // Alice, Bob, and Carol

        bb.close();
    }

    @Test
    public void skip_to_internal_offset_simple_triple_pattern () throws RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BlazegraphDistinctIteratorDXV itBase = new BlazegraphDistinctIteratorDXV(bb.store,
                any, address, any, any,
                Set.of(SPOC.SUBJECT));

        List<Long> offsets = new ArrayList<>(); // making sure that the offset are actually the proper ones
        while (itBase.hasNext()) {
            itBase.next();
            log.debug("Base offset: {} for {}", itBase.current(), itBase.getString(SPOC.SUBJECT));
            offsets.add(itBase.current());
        }

        for (int i = 0; i < offsets.size(); ++i ){
            BlazegraphDistinctIteratorDXV it = new BlazegraphDistinctIteratorDXV(bb.store,
                    any, address, any, any,
                    Set.of(SPOC.SUBJECT));

            it.skip(offsets.get(i));
            long nbResults = 0;
            while (it.hasNext()) {
                it.next();
                ++nbResults;
                log.debug("offset {} for {}", it.current(), it.getString(SPOC.SUBJECT));
            }
            assertEquals(offsets.size() - i - 1, nbResults);
        }

        bb.close();
    }

    @Test
    public void skipping_above_max_offset_should_return_no_result() throws RepositoryException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BlazegraphDistinctIteratorDXV itBase = new BlazegraphDistinctIteratorDXV(bb.store,
                any, address, any, any,
                Set.of(SPOC.SUBJECT));

        List<Long> offsets = new ArrayList<>(); // making sure that the offset are actually the proper ones
        while (itBase.hasNext()) {
            itBase.next();
            offsets.add(itBase.current());
        }
        long maxOffset = offsets.get(offsets.size()-1);
        log.debug("Max offset: {} for {}", maxOffset, itBase.getString(SPOC.SUBJECT));

        BlazegraphDistinctIteratorDXV it = new BlazegraphDistinctIteratorDXV(bb.store,
                any, address, any, any,
                Set.of(SPOC.SUBJECT));

        it.skip(maxOffset + 10);

        assertFalse(it.hasNext());

        bb.close();
    }


    /* ******************************** ON BIG DATASETS *************************************** */

    @Test
    public void get_distinct_s_over_a_triple_pattern_with_lot_of_values () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        Assumptions.assumeTrue(Objects.nonNull(BlazegraphBackendTest.watdiv));
        BlazegraphBackend bb = BlazegraphBackendTest.watdiv;

        long startBaseline = System.currentTimeMillis();
        Multiset<BindingSet> results = bb.executeQuery("SELECT DISTINCT ?s WHERE {?s <http://schema.org/eligibleRegion> ?o}");
        long elapsedBaseline = System.currentTimeMillis() - startBaseline;

        IV address = bb.getId("<http://schema.org/eligibleRegion>", SPOC.PREDICATE);
        IV any = bb.any();

        BlazegraphDistinctIteratorDXV it = new BlazegraphDistinctIteratorDXV(bb.store,
                any, address, any, any,
                Set.of(SPOC.SUBJECT)); // distinct (?s)

        long start = System.currentTimeMillis();
        long nbResults = 0L;
        while (it.hasNext()) {
            it.next();
            ++nbResults;
        }
        long elapsed = System.currentTimeMillis() - start;

        // expected to be a lot slower our iterator
        log.debug("Baseline: Took {}ms to get {} distinct results", elapsedBaseline, results.size());
        log.debug("Ours: Took {}ms to get {} distinct results", elapsed, nbResults);

        assertEquals(results.size(), nbResults);
    }

    @Disabled(value = "Time consuming.")
    @Test
    public void get_distinct_s_over_a_triple_pattern_with_lot_of_values_on_wdbench () throws RepositoryException, QueryEvaluationException, MalformedQueryException {
        Assumptions.assumeTrue(Objects.nonNull(BlazegraphBackendTest.wdbench));
        BlazegraphBackend bb = BlazegraphBackendTest.wdbench;
        // unfortunately, blazegraph often fails to provide an answer because it eats too much memory which
        // ends up in throwing…
        //        long startBaseline = System.currentTimeMillis();
        //        long nbResultsBaseline = bb.countQuery("SELECT DISTINCT ?s WHERE {?s <http://www.wikidata.org/prop/direct/P31> ?o}");
        //        long elapsedBaseline = System.currentTimeMillis() - startBaseline;
        IV address = bb.getId("<http://www.wikidata.org/prop/direct/P31>", SPOC.PREDICATE);

        BlazegraphDistinctIteratorDXV it = new BlazegraphDistinctIteratorDXV(bb.store,
                bb.any(), address, bb.any(), bb.any(),
                Set.of(SPOC.SUBJECT)); // distinct (?s)

        long start = System.currentTimeMillis();
        long nbResults = 0L;
        while (it.hasNext()) {
            it.next();
            ++nbResults;
        }
        long elapsed = System.currentTimeMillis() - start;

        // expected to be a lot slower our iterator
        // log.debug("Baseline: Took {}ms to get {} distinct results", elapsedBaseline, nbResultsBaseline);
        log.debug("Ours: Took {}ms to get {} distinct results", elapsed, nbResults);
        // [main] DEBUG BlazegraphDistinctIterator2Test - Ours: Took 723431ms to get 90392189 distinct results

        // assertEquals(nbResultsBaseline, nbResults);
    }


    @Test
    public void get_distinct_through_blazegraph_backend_and_count_distinct_predicates_of_wdbench() {
        Assumptions.assumeTrue(Objects.nonNull(BlazegraphBackendTest.wdbench));
        BlazegraphBackend bb = BlazegraphBackendTest.wdbench;
        var it = bb.searchDistinct(bb.any(), bb.any(), bb.any(), Set.of(SPOC.PREDICATE));

        long start = System.currentTimeMillis();
        long nbResults = 0L;
        while (it.hasNext()) {
            it.next();
            ++nbResults;
        }
        long elapsed = System.currentTimeMillis() - start;

        // expected to be a lot slower our iterator
        // log.debug("Baseline: Took {}ms to get {} distinct results", elapsedBaseline, nbResultsBaseline);
        log.debug("Ours: Took {}ms to get {} distinct results", elapsed, nbResults);
        assertEquals(8604L, nbResults); // from the paper for count-distinct queries (CRAWD)
        // [main] DEBUG BlazegraphDistinctIterator2Test - Ours: Took 1186ms to get 8604 distinct results
    }

    @Disabled(value = "Time consuming.")
    @Test
    public void get_distinct_through_blazegraph_backend_and_count_distinct_subjects_of_wdbench() {
        Assumptions.assumeTrue(Objects.nonNull(BlazegraphBackendTest.wdbench));
        BlazegraphBackend bb = BlazegraphBackendTest.wdbench;
        var it = bb.searchDistinct(bb.any(), bb.any(), bb.any(), Set.of(SPOC.SUBJECT));

        long start = System.currentTimeMillis();
        long nbResults = 0L;
        while (it.hasNext()) {
            it.next();
            ++nbResults;
        }
        long elapsed = System.currentTimeMillis() - start;

        // expected to be a lot slower our iterator
        // log.debug("Baseline: Took {}ms to get {} distinct results", elapsedBaseline, nbResultsBaseline);
        log.debug("Ours: Took {}ms to get {} distinct results", elapsed, nbResults);
        // With first iterator:
        // [main] DEBUG BlazegraphDistinctIterator2Test - Ours: Took 738058ms to get 92498623 distinct results
        // With alternative iterator:
        // [main] DEBUG BlazegraphDistinctIterator2Test - Ours: Took 712483ms to get 92498623 distinct results
        assertEquals(92_498_623L, nbResults); // from the paper for count-distinct queries (CRAWD)
    }
}
