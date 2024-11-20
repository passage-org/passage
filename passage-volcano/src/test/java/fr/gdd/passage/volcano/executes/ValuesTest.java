package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValuesTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void undef_in_values () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT * WHERE {  VALUES ?p { UNDEF } }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // 1 result but empty. (Wikidata's online server confirms)
    }

    @Test
    public void values_with_a_term_that_does_not_exist_in_database () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT * WHERE {  VALUES ?p { <http://does_not_exist> } }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // the one that does not exist
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p"), List.of("does_not_exist")));
    }

    @Test
    public void values_with_a_term_that_does_not_exist_put_in_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
                SELECT * WHERE {
                    VALUES ?p { <http://does_not_exist> }
                    ?p <http://address> ?c
                }""";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(0, results.size()); // does not exist
    }

    @Test
    public void simple_values_with_nothing_else () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = "SELECT * WHERE { VALUES ?p { <http://Alice> }  }";

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // Alice lives in Nantes
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p"), List.of("Alice")));
    }

    @Test
    public void simple_values_with_only_one_variable_value () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // Alice lives in Nantes
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
    }

    @Test
    public void simple_values_with_only_one_variable_but_multiple_values () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(2, results.size()); // Alice lives in Nantes; Bob lives in Paris
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Bob", "paris")));
    }

    @Test
    public void values_that_do_not_exist () throws RepositoryException { // TODO
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> <http://Coralie>}
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(2, results.size()); // Alice lives in Nantes; Bob lives in Paris
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "c"), List.of("Bob", "paris")));
    }

    @Test
    public void an_empty_values() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                VALUES ?p { }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(0, results.size()); // Nothing
    }

    @Test
    public void value_in_the_middle() throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                ?p2 <http://address> ?c
                VALUES ?p { <http://Alice> }
                ?p <http://address> ?c
            }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(2, results.size()); // (Alice and herself + Alice and Carol) live in nantes
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "p2", "c"), List.of("Alice", "Alice", "nantes")));
        assertTrue(MultisetResultChecking.containsResult(results, List.of("p", "p2", "c"), List.of("Alice", "Carol", "nantes")));
    }

    @Test
    public void caerthesian_product_with_values () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                ?person <http://address> ?city
                VALUES ?location { <http://France> <http://Europe> }
            }
        """;

        var results = OpExecutorUtils.executeWithPassage(query, blazegraph);
        assertEquals(6, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "city", "location"),
                List.of("Alice", "nantes", "France"),
                List.of("Bob", "paris", "France"),
                List.of("Carol", "nantes", "France"),
                List.of("Alice", "nantes", "Europe"),
                List.of("Bob", "paris", "Europe"),
                List.of("Carol", "nantes", "Europe")));
    }

}
