package fr.gdd.passage.hdt;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.hdt.datasets.HDTInMemoryDatasetsFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class HDTBackendTest {

    private final static Logger log = LoggerFactory.getLogger(HDTBackendTest.class);

    @Test
    public void create_a_small_inmemory_dataset () throws Exception {
        HDTBackend backend = new HDTBackend(HDTInMemoryDatasetsFactory.triples9());
        HDTIterator it = (HDTIterator) backend.search(backend.any(), backend.any(), backend.any());
        int count = 0;
        while (it.hasNext()) {
            it.next();
            log.debug("spo = {} {} {}", it.getString(SPOC.SUBJECT), it.getString(SPOC.PREDICATE), it.getString(SPOC.OBJECT));
            count += 1;
        }
        assertEquals(9, count);

        Long aliceId = backend.getId("<http://Alice>", SPOC.SUBJECT);

        assertTrue(aliceId > 0L);

        HDTIterator it2 = (HDTIterator) backend.search(aliceId, backend.any(), backend.any());
        count = 0;
        while (it2.hasNext()) {
            it2.next();
            ++count;
        }

        assertEquals(4, count);
        backend.close();
    }

    @Test
    public void element_not_found () throws Exception {
        HDTBackend backend = new HDTBackend(HDTInMemoryDatasetsFactory.triples9());
        assertThrows(NotFoundException.class, () ->  backend.getId("not exists", SPOC.PREDICATE));

        // It should throw because Alice does not exist as predicate, only as subject
        assertThrows(NotFoundException.class, () ->  backend.getId("<http://Alice>", SPOC.PREDICATE));
        backend.close();
    }

    @Test
    public void may_i_skip_some_elements_of_iterator () throws Exception {
        HDTBackend backend = new HDTBackend(HDTInMemoryDatasetsFactory.triples9());
        HDTIterator all = (HDTIterator) backend.search(backend.any(), backend.any(), backend.any());

        all.skip(5L); // -5 results out of 9
        int count = 0;
        while (all.hasNext()) {
            all.next();
            ++count;
        }
        assertEquals(4, count);
        backend.close();
    }

    @Disabled("Only to try which indexes allow skipping.")
    @Test
    public void skip_on_each_kind_of_iterator () throws Exception {
        // TODO this does not pass, because the hdt-java might have an issue, see
        //  for more info: <https://github.com/Chat-Wane/passage/issues/7>
        // HDTBackend backend = new HDTBackend(IM4HDT.triples9());
        HDTBackend backend = new HDTBackend("C:\\Users\\brice\\IdeaProjects\\passage\\passage-hdt\\src\\test\\java\\fr\\gdd\\passage\\hdt\\watdiv.10M.hdt");
        HDTManager.indexedHDT(backend.hdt, null);
        Long any = backend.any();
        // Long sId = backend.getId("<http://Alice>", SPOC.SUBJECT);
        Long sId = backend.getId("http://db.uwaterloo.ca/~galuc/wsdbm/City1", SPOC.SUBJECT);
        // Long pId = backend.getId("<http://own>", SPOC.PREDICATE);
        Long pId = backend.getId("http://www.geonames.org/ontology#parentCountry", SPOC.PREDICATE);
        // Long oId = backend.getId( "<http://canine>", SPOC.OBJECT);
        Long oId = backend.getId("http://db.uwaterloo.ca/~galuc/wsdbm/Country23", SPOC.OBJECT);

        HDTIterator it;

        it = (HDTIterator) backend.search(any, any, any);
        it.skip(1L);

        it = (HDTIterator) backend.search(sId, any, any);
        it.skip(1L);

        // it = (HDTIterator) backend.search(any, pId, any);
        // it.skip(1L);

        // it = (HDTIterator) backend.search(any, any, oId);
        // it.skip(1L);

        // it = (HDTIterator) backend.search(sId, pId, any);
        // it.skip(1L);

        // it = (HDTIterator) backend.search(sId, any, oId);
        // it.skip(1L);

        // it = (HDTIterator) backend.search(any, pId, oId);
        // it.skip(1L);
        backend.close();
    }

    @Disabled("Cannot skip on any triple pattern index.")
    @Test
    public void getting_random_elements_from_an_iterator () throws Exception {
        HDTBackend backend = new HDTBackend(HDTInMemoryDatasetsFactory.triples9());
        Long aliceId = backend.getId("<http://Alice>", SPOC.SUBJECT);
        HDTIterator it = (HDTIterator) backend.search(aliceId, backend.any(), backend.any());

        HashSet<String> objects = new HashSet<>();
        int count = 0;
        while (count < 100) {
            double proba = it.random();
            assertEquals(1./4., proba);
            it.next();
            objects.add(it.getValue(SPOC.OBJECT));
            count += 1;
        }

        assertEquals(4, objects.size());
        log.debug("{}", objects);
        backend.close();
    }


    @Disabled("Only here to assess the time required to go to a particular index.")
    @Test
    public void count_all_elements_using_has_next_next () throws Exception {
        // HDTBackend backend = new HDTBackend("/Users/skoazell/Desktop/Projects/datasets/wdbench-hdt/wdbench.hdt");
        HDTBackend backend = new HDTBackend("/Users/skoazell/Desktop/Projects/datasets/watdiv10m-hdt/watdiv.10M.hdt");
        HDTManager.indexedHDT(backend.hdt, null);
        final long SKIP = 10_000_000L; // roughly the end of the dataset on spo

        long start = System.currentTimeMillis();
        var any = backend.any();
        for (int i = 0; i < 100; i++) {
            var it = (HDTIterator) backend.search(any, any, any);
            it.skip(SKIP);

        }
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Took {}ms to skip to {}", elapsed, SKIP);

//        long remaining = 0L;
//        while (it.hasNext()) {
//            it.next();
//            remaining++;
//        }
//        log.debug("remaining {}", remaining);

        backend.close();
    }
}