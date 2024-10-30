package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BGPTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void a_literal_at_predicate_position () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { "12" }
                    ?p ?predicate ?c
                }""";

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size());
    }

    @Test
    public void a_tp_with_an_unknown_value () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://does_not_exist> ?c}";

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size());
    }

    @Test
    public void a_bgp_with_an_unknown_value () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://does_not_exist> ?c}";

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size());
    }

    @Test
    void an_unknown_value_at_first_but_then_known () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { <http://does_not_exist> <http://address> }
                    ?p ?predicate ?c}
                """;

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size());
    }

    @Test
    void an_unknown_value_at_first_but_then_known_but_known_first () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { <http://address> <http://does_not_exist> }
                    ?p ?predicate ?c}
                """;

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size());
    }

    @Test
    public void bgp_of_1_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Bob, Alice, and Carol.
    }

    @Test
    public void bgp_of_2_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
    }

    @Test
    public void bgp_of_3_tps () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
                ?a <http://species> ?s
               }""";

        var results = PassageOpExecutorTest.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice->own->cat,dog,snake
    }

}
