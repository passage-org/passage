package fr.gdd.passage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.interfaces.SPOC;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BlazegraphDistinctIteratorTest {

    private static Logger log = LoggerFactory.getLogger(BlazegraphDistinctIteratorTest.class);

    @Test
    public void look_for_distinct_of_fully_bounded_triple_pattern () throws RepositoryException, SailException {
        // fully bounded in the sense that everything is either a projected variable, or a constant.
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, address, any, Set.of(SPOC.SUBJECT, SPOC.OBJECT));

        int count = 0;
        while (it.hasNext()) {
            it.next();
            log.debug("{}: {} {}", count, it.getString(SPOC.SUBJECT), it.getString(SPOC.OBJECT));
            count++;
        }

        assertEquals(3, count); // Alice nantes, Bob paris, Carol nantes
        bb.close();
    }

    @Test
    public void look_for_distinct_of_fully_bounded_quad_pattern () throws RepositoryException, SailException {
        // fully bounded in the sense that everything is either a projected variable, or a constant.
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, address, any, any, Set.of(SPOC.SUBJECT, SPOC.OBJECT, SPOC.GRAPH));
        int count = 0;
        while (it.hasNext()) {
            it.next();
            log.debug("{}: {} {} {}", count, it.getString(SPOC.SUBJECT), it.getString(SPOC.OBJECT), it.getString(SPOC.GRAPH));
            count++;
        }

        assertEquals(3, count); // Alice nantes, Bob paris, Carol nantes
        bb.close();
    }

    @Test
    public void look_for_distinct_for_object_of_triple_pattern () throws RepositoryException, SailException {
        // fully bounded in the sense that everything is either a projected variable, or a constant.
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        IV address = bb.getId("<http://address>", SPOC.PREDICATE);
        IV any = bb.any();

        BackendIterator<IV, BigdataValue> it = bb.searchDistinct(any, address, any, Set.of(SPOC.OBJECT));

        int count = 0;
        while (it.hasNext()) {
            it.next();
            log.debug("{}: {}", count, it.getString(SPOC.OBJECT));
            count++;
        }

        assertEquals(2, count); // nantes paris
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

        bb.close();
    }

}
