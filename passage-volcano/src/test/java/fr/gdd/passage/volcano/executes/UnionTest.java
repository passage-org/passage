package fr.gdd.passage.volcano.executes;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnionTest {

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void execute_a_simple_union () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?a}
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(6, results.size()); // 3 triples + 3 triples
        assertTrue(OpExecutorUtils.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "snake"), List.of("Alice", "dog"), List.of("Alice", "cat"),
                List.of("Alice", "nantes"), List.of("Bob", "paris"), List.of("Carol", "nantes")));
    }

    @Test
    public void execute_a_union_inside_a_triple_pattern () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p  <http://own>  ?a .
                {?a <http://species> ?s} UNION {?a <http://species> ?s}
               }""";

        var results = OpExecutorUtils.executeWithPassage(queryAsString, blazegraph);
        assertEquals(6, results.size()); // (cat + dog + snake)*2
        assertTrue(OpExecutorUtils.containsResultTimes(results, List.of("p", "s"),
                List.of("Alice", "feline"), 2));
        assertTrue(OpExecutorUtils.containsResultTimes(results, List.of("p", "s"),
                List.of("Alice", "canine"), 2));
        assertTrue(OpExecutorUtils.containsResultTimes(results, List.of("p", "s"),
                List.of("Alice", "reptile"), 2));
    }

}
