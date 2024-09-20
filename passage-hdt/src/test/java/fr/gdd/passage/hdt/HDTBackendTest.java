package fr.gdd.passage.hdt;

import com.bigdata.rdf.spo.SPO;
import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.interfaces.SPOC;
import fr.gdd.passage.databases.inmemory.IM4HDT;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.rdfhdt.hdt.hdt.HDTFactory;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.TripleID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class HDTBackendTest {

    private final static Logger log = LoggerFactory.getLogger(HDTBackendTest.class);

    @Test
    public void create_a_small_inmemory_dataset () {
        HDTBackend backend = new HDTBackend(IM4HDT.triples9());
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
    }

    @Test
    public void element_not_found () {
        HDTBackend backend = new HDTBackend(IM4HDT.triples9());
        assertThrows(NotFoundException.class, () ->  backend.getId("not exists", SPOC.PREDICATE));

        // It should throw because Alice does not exist as predicate, only as subject
        assertThrows(NotFoundException.class, () ->  backend.getId("<http://Alice>", SPOC.PREDICATE));
    }

    @Test
    public void may_i_skip_some_elements_of_iterator () {
        HDTBackend backend = new HDTBackend(IM4HDT.triples9());
        HDTIterator all = (HDTIterator) backend.search(backend.any(), backend.any(), backend.any());

        all.skip(5L); // -5 results out of 9
        int count = 0;
        while (all.hasNext()) {
            all.next();
            ++count;
        }
        assertEquals(4, count);
    }

    @Disabled
    @Test
    public void skip_on_each_kind_of_iterator () throws IOException {
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
    }

    @Test
    public void getting_random_elements_from_an_iterator () {
        HDTBackend backend = new HDTBackend(IM4HDT.triples9());
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
    }

}