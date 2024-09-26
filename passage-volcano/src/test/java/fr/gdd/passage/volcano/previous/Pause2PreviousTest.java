package fr.gdd.passage.volcano.previous;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class Pause2PreviousTest {

    private static final Logger log = LoggerFactory.getLogger(Pause2PreviousTest.class);

    @Test
    public void simple_triple_pattern () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        // TODO TODO TODO
    }

}