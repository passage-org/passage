package fr.gdd.passage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class BlazegraphDistinctIteratorTest {

    private static Logger log = LoggerFactory.getLogger(BlazegraphDistinctIteratorTest.class);

    @Test
    public void get_DXD () throws RepositoryException, SailException {
        // fully bounded in the sense that everything is either a projected variable, or a constant.
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, address, any, Set.of(SPOC.SUBJECT, SPOC.OBJECT));
        assertInstanceOf(BlazegraphIterator.class, it); // since fully bound
        int count = executeAndCount(it);

        assertEquals(3, count); // Alice nantes, Bob paris, Carol nantes
        bb.close();
    }

    @Test
    public void get_DXDD () throws RepositoryException, SailException {
        // fully bounded in the sense that everything is either a projected variable, or a constant.
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, address, any, any, Set.of(SPOC.SUBJECT, SPOC.OBJECT, SPOC.GRAPH));
        assertInstanceOf(BlazegraphIterator.class, it); // fully bound
        int count = executeAndCount(it);

        assertEquals(3, count); // Alice nantes, Bob paris, Carol nantes
        bb.close();
    }

    @Test
    public void get_VXD () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, address, any, Set.of(SPOC.OBJECT));
        assertInstanceOf(BlazegraphDistinctIteratorXDV.class, it); // Pocs => XDVV
        int count = executeAndCount(it);

        assertEquals(2, count); // nantes paris
        bb.close();
    }

    @Test
    public void get_DXV () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, address, any, Set.of(SPOC.SUBJECT));
        // since C is set in stone because search of triples, we should use PCso, which is efficient
        // but for now, it uses SPoc.
        assertInstanceOf(BlazegraphDistinctIteratorDXV.class, it); // uses sPoc
        int count = executeAndCount(it);

        assertEquals(3, count); // alice, carol, bob
        bb.close();
    }

    @Test
    public void get_VVVD () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, any, any, any, Set.of(SPOC.GRAPH));
        assertInstanceOf(BlazegraphDistinctIteratorXDV.class, it); // uses cspo
        int count = executeAndCount(it);

        assertEquals(3, count);
        bb.close();
    }

    @Disabled ("TODO")
    @Test
    public void get_DXV_where_D_is_bounded () throws RepositoryException, SailException {
        // TODO TODO 
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.graph3());
        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV alice = bb.getId("<http://Alice>", SPOC.SUBJECT);
        IV any = bb.any();

        // TODO do something about it. How do I ask for this iterator?
        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(alice, address, any, Set.of(SPOC.SUBJECT));
        int count = executeAndCount(it);

        assertEquals(1, count); // It simply states that it exists…
        bb.close();
    }

    /* ********************************************************************************** */

    @Test
    public void on_watdiv_fully_bounded () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Objects.nonNull(BlazegraphBackendTest.watdiv));
        BlazegraphBackend bb = BlazegraphBackendTest.watdiv;

        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, any, any, any, Set.of(SPOC.SUBJECT, SPOC.PREDICATE, SPOC.OBJECT, SPOC.GRAPH));
        int count = 0;
        assertTrue(it.hasNext());
        it.next();
        log.debug("{}: {} {} {} {}", count,
                it.getString(SPOC.SUBJECT),
                it.getString(SPOC.PREDICATE),
                it.getString(SPOC.OBJECT),
                it.getString(SPOC.GRAPH));
    }

    /* ************************************************************************************* */

    public static int executeAndCount(BackendIterator<IV, BigdataValue> it) {
        int count = 0;
        while (it.hasNext()) {
            it.next();
            ++count;
            log.debug("{}: {} {} {} {}", count,
                    codeToString(it, SPOC.SUBJECT),
                    codeToString(it, SPOC.PREDICATE),
                    codeToString(it, SPOC.OBJECT),
                    codeToString(it, SPOC.GRAPH));
        }

        return count;
    }

    private static String codeToString(BackendIterator<IV, BigdataValue> it, int code) {
        try {
            return it.getString(code);
        } catch (Exception e) {
            return "_";
        }
    }

}
