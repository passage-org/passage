package fr.gdd.passage.volcano.spliterators;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PassageSplitScanTest {

    private static final Logger log = LoggerFactory.getLogger(PassageSplitScan.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageSplitScan.stopping = (e) -> false; }

    @Test
    public void a_simple_test_of_triple_pattern () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        var context = new PassageExecutionContextBuilder().setBackend(backend).build();

        OpTriple tp = new OpTriple(Triple.create(
                Var.alloc("s"),
                NodeFactory.createURI("http://own"),
                Var.alloc("a")));
        var it = new PassageSplitScan<>(context, new BackendBindings<>(), tp);

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        StreamSupport.stream(it, true).forEach(results::add);
        log.debug("{}", results);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
    }

    @Test
    public void a_simple_test_of_bgp () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        var context = new PassageExecutionContextBuilder().setBackend(backend).build();

        OpTriple tp1 = new OpTriple(Triple.create(
                Var.alloc("s"),
                NodeFactory.createURI("http://address"),
                Var.alloc("c")));

        OpTriple tp2 = new OpTriple(Triple.create(
                Var.alloc("s"),
                NodeFactory.createURI("http://own"),
                Var.alloc("a")));

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        StreamSupport.stream(new PassageSplitScan<>(context, new BackendBindings<>(), tp1), true)
                .forEach((mu)-> StreamSupport.stream(new PassageSplitScan<>(context, mu, tp2), true)
                        .forEach(results::add));
        log.debug("{}", results);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "a", "c"),
                List.of("Alice", "cat", "nantes"),
                List.of("Alice", "dog", "nantes"),
                List.of("Alice", "snake", "nantes")));
    }

    @Test
    public void a_first_backjump_test () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        var context = new PassageExecutionContextBuilder().setBackend(backend).build();

        OpTriple tp2 = new OpTriple(Triple.create(
                Var.alloc("s"),
                Var.alloc("p"),
                Var.alloc("o")));

        OpTriple tp1 = new OpTriple(Triple.create(
                Var.alloc("s"),
                NodeFactory.createURI("http://address"),
                Var.alloc("c")));

        OpTriple tp3 = new OpTriple(Triple.create(
                Var.alloc("s"),
                NodeFactory.createURI("http://own"),
                Var.alloc("a")));

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        StreamSupport.stream(new PassageSplitScan<>(context, new BackendBindings<>(), tp1), false)
                .forEach((mu)-> StreamSupport.stream(new PassageSplitScan<>(context, mu, tp2), false)
                        .forEach((mu2)-> StreamSupport.stream(new PassageSplitScan<>(context, mu2, tp3), false)
                                .forEach(results::add)));
        log.debug("{}", results);
        // enumerate all predicates `po` of alice for each animal: 4x3
        assertEquals(12, results.size());
    }
}