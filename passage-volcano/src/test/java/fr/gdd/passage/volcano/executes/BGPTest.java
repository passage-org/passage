package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.benchmarks.WatDivTest;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BGPTest {

    final static Logger log = LoggerFactory.getLogger(BGPTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void a_literal_at_predicate_position () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { "12" }
                    ?p ?predicate ?c
                }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size());
    }

    @Test
    public void a_tp_with_an_unknown_value () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://does_not_exist> ?c}";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size());
    }

    @Test
    public void a_bgp_with_an_unknown_value () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://does_not_exist> ?c}";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(0, results.size());
    }

    @Test
    void an_unknown_value_at_first_but_then_known () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { <http://does_not_exist> <http://address> }
                    ?p ?predicate ?c}
                """;

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "predicate", "c"),
                List.of("Alice", "address", "nantes"),
                List.of("Bob", "address", "paris"),
                List.of("Carol", "address", "nantes")));
    }

    @Test
    void an_unknown_value_at_first_but_then_known_but_known_first () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { <http://address> <http://does_not_exist> }
                    ?p ?predicate ?c}
                """;

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "predicate", "c"),
                List.of("Alice", "address", "nantes"),
                List.of("Bob", "address", "paris"),
                List.of("Carol", "address", "nantes")));
    }

    @Test
    public void bgp_of_1_tp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Bob, Alice, and Carol.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
    }

    @Test
    public void bgp_of_2_tps () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "dog"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
    }

    @Test
    public void bgp_of_3_tps () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
                ?a <http://species> ?s
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(3, results.size()); // Alice->own->cat,dog,snake
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "snake", "reptile")));
    }


    @Disabled("Time consuming.")
    @Test
    public void sandbox_of_test () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WatDivTest.PATH);

        String query = """        
                SELECT ?v7 ?v1 ?v5 ?v6 ?v0 ?v3 ?v2 WHERE {
                        ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/Genre13>.
                        ?v2 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?v0.
                        ?v2 <http://schema.org/caption> ?v5.
                        ?v2 <http://schema.org/keywords> ?v7.
                        ?v2 <http://schema.org/contentRating> ?v6.
                        ?v2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?v3.
                        ?v0 <http://ogp.me/ns#tag> ?v1.
                }
                """;

        int sum = OpExecutorUtils.executeWithPassage(query, watdivBlazegraph).size();
        log.info("{}", sum);
    }

    @Disabled("Time consuming.")
    @Test
    public void on_watdiv_conjunctive_query_10124 () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend(WatDivTest.PATH);

        String query0 = """
                SELECT * WHERE {
                        ?v1 <http://www.geonames.org/ontology#parentCountry> ?v2.
                        ?v3 <http://purl.org/ontology/mo/performed_in> ?v1.
                        ?v0 <http://purl.org/dc/terms/Location> ?v1.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1>.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId> ?v5.
                        ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows> ?v0.
                }
                """;

        int sum = OpExecutorUtils.executeWithPassage(query0, watdivBlazegraph).size();

        assertEquals(117, sum);
    }

}
