package fr.gdd.passage.volcano;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValuesTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void simple_values_with_only_one_variable_value () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> }
                ?p <http://address> ?c
            }
        """;

        var results = PassageOpExecutorTest.executeWithPassage(query, blazegraph);
        assertEquals(1, results.size()); // Alice lives in Nantes
        assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
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

        var results = PassageOpExecutorTest.executeWithPassage(query, blazegraph);
        assertEquals(2, results.size()); // Alice lives in Nantes; Bob lives in Paris
        assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "c"), List.of("Bob", "paris")));
    }

    @Disabled
    @Test
    public void values_that_do_not_exist () throws RepositoryException { // TODO
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                VALUES ?p { <http://Alice> <http://Bob> <http://Coralie>}
                ?p <http://address> ?c
            }
        """;

        var results = PassageOpExecutorTest.executeWithPassage(query, blazegraph);
        assertEquals(2, results.size()); // Alice lives in Nantes; Bob lives in Paris
        assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "c"), List.of("Alice", "nantes")));
        assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "c"), List.of("Bob", "paris")));
    }

    @Disabled
    @Test
    public void an_empty_values() throws RepositoryException {
        // TODO: should it return nothing or should it return as if unbound ?
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String query = """
            SELECT * WHERE {
                VALUES ?p { }
                ?p <http://address> ?c
            }
        """;

        var results = PassageOpExecutorTest.executeWithPassage(query, blazegraph);
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

        var results = PassageOpExecutorTest.executeWithPassage(query, blazegraph);
        assertEquals(2, results.size()); // (Alice and herself + Alice and Carol) live in nantes
        assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "p2", "c"), List.of("Alice", "Alice", "nantes")));
        assertTrue(PassageOpExecutorTest.containsResult(results, List.of("p", "p2", "c"), List.of("Alice", "Carol", "nantes")));
    }

}
