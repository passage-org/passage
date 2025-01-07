package fr.gdd.passage.blazegraph;

import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class BlazegraphDistinctIteratorTest {

    @Test
    public void look_for_distinct_of_fully_bounded_triple_pattern () throws RepositoryException, SailException {
        // fully bounded in the sense that everything is either a projected variable, or a constant.
        // TODO TODO TODO
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        BigdataValue twelve = bb.getValue("\"12\"");
        assertInstanceOf(BigdataLiteral.class, twelve);
        assertEquals("\"12\"", twelve.toString());
        BigdataValue uri = bb.getValue("<https://uri>");
        assertInstanceOf(BigdataURI.class, uri);
        assertEquals("<https://uri>", uri.toString());
        bb.close();
    }

}
