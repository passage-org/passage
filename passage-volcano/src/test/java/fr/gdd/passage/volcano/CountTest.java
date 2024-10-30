package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled("Count aggregates not supported yet.")
public class CountTest {

    static final Logger log = LoggerFactory.getLogger(CountTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void simple_count_on_a_single_triple_pattern() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT (COUNT(*) AS ?count) { ?p <http://address> ?c }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // ?count = 3
    }

    @Test
    public void simple_count_on_a_single_triple_pattern_driven_by_another_one() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
        SELECT * WHERE {
            ?p <http://address> ?c .
            {SELECT (COUNT(*) AS ?count) { ?p <http://own> ?animal }}
        }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(3, results.size()); // ?count = 3 for Alice; Bob and Carol have ?count = 0
    }

}
