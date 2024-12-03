package fr.gdd.passage.volcano.push.execute;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.streams.PassageSplitScan;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class PassageSplitScanTest {

    private static final Logger log = LoggerFactory.getLogger(PassageSplitScanTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageSplitScan.stopping = (e) -> false; }

    @Test
    public void correct_offset_limits_for_split_tps () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        var context = new PassageExecutionContextBuilder().setBackend(backend).build();

        OpTriple tp = new OpTriple(Triple.create(
                Var.alloc("s"),
                NodeFactory.createURI("http://own"),
                Var.alloc("a")));

        context.setOffset(1L);

        var it = new PassageSplitScan<>(context, new BackendBindings<>(), tp);
        PassageSplitScan it2 = (PassageSplitScan) it.trySplit();

        assertEquals(1, it.getOffset());
        assertEquals(1, it.estimateSize());
        assertEquals(1, it.getLimit());

        assertEquals(2, it2.getOffset());
        assertEquals(1, it2.estimateSize());
        assertNull(it2.getLimit());

        // same but because we advanced in production
        context.setOffset(0L);
        var it3 = new PassageSplitScan<>(context, new BackendBindings<>(), tp);
        it3.tryAdvance((a) -> {log.debug("skip {}", a);});
        PassageSplitScan it4 = (PassageSplitScan) it3.trySplit();
        assertEquals(1, it3.getOffset());
        assertEquals(1, it3.estimateSize());
        assertEquals(1, it3.getLimit());

        assertEquals(2, it4.getOffset());
        assertEquals(1, it4.estimateSize());
        assertNull(it4.getLimit());
    }

    @Test
    public void split_of_split() throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        var context = new PassageExecutionContextBuilder().setBackend(backend).build();

        OpTriple tp = new OpTriple(Triple.create(
                NodeFactory.createURI("http://Alice"),
                Var.alloc("p"),
                Var.alloc("o")));

        // context.setOffset(0L);

        // nantes ]/ cat / dog / snake
        var it1 = new PassageSplitScan<>(context, new BackendBindings<>(), tp);
        var it2 = (PassageSplitScan) it1.trySplit();

        assertEquals(4, it1.estimateSize() + it2.estimateSize());
        log.debug("it1 offset {}", it1.getOffset());
        log.debug("it2 offset {}", it2.getOffset());
        var it3 = (PassageSplitScan) it1.trySplit();
        assertEquals(4, it3.estimateSize() + it1.estimateSize() + it2.estimateSize());
        log.debug("it1 offset {}", it1.getOffset());
        log.debug("it3 offset {}", it3.getOffset());
        log.debug("it2 offset {}", it2.getOffset());
        var it4 = (PassageSplitScan) it2.trySplit();
        assertEquals(4, it3.estimateSize() + it1.estimateSize() + it2.estimateSize() + it4.estimateSize());
        log.debug("it1 offset {}", it1.getOffset());
        log.debug("it3 offset {}", it3.getOffset());
        log.debug("it2 offset {}", it2.getOffset());
        log.debug("it4 offset {}", it4.getOffset());
        it1.forEachRemaining(e -> log.debug("1: {}", e.toString()));
        it3.forEachRemaining(e -> log.debug("3: {}", e.toString()));
        it2.forEachRemaining(e -> log.debug("2: {}", e.toString()));
        it4.forEachRemaining(e -> log.debug("4: {}", e.toString()));
    }

    @RepeatedTest(20)
    public void simple_bgp_to_test () throws RepositoryException, SailException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
            var context = new PassageExecutionContextBuilder().setBackend(backend).build();

            OpTriple tp = new OpTriple(Triple.create(
                    Var.alloc("s"),
                    NodeFactory.createURI("http://own"),
                    Var.alloc("a")));

            Multiset<BackendBindings<?, ?>> results = ConcurrentHashMultiset.create();
            try (ForkJoinPool customPool = new ForkJoinPool(4)) {
                customPool.submit(() ->
                        StreamSupport.stream(new PassageSplitScan<>(context, new BackendBindings<>(), tp), true)
                                .forEach(results::add)
                ).join();
            }
            log.debug("{}", results);
            assertEquals(3, results.size());
            assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "a"),
                    List.of("Alice", "cat"),
                    List.of("Alice", "dog"),
                    List.of("Alice", "snake")));
            backend.close();
    }

    @RepeatedTest(20)
    public void a_simple_test_of_bgp () throws RepositoryException, SailException {
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

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();

        try (ForkJoinPool customPool = new ForkJoinPool(4)) {
            customPool.submit(() ->
                    StreamSupport.stream(new PassageSplitScan<>(context, new BackendBindings<>(), tp1), true)
                            .forEach((mu) -> StreamSupport.stream(new PassageSplitScan<>(context, mu, tp2), true)
                                    .forEach(results::add))
            ).join();
        }
        log.debug("{}", results);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("s", "a", "c"),
                List.of("Alice", "cat", "nantes"),
                List.of("Alice", "dog", "nantes"),
                List.of("Alice", "snake", "nantes" )));
        backend.close();
    }

    @RepeatedTest(20)
    public void a_first_backjump_test_where_tp3_throws_catch_by_tp1 () throws RepositoryException, SailException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        var context = new PassageExecutionContextBuilder().setMaxParallel(10).setBackend(backend).build();

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

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        try (ForkJoinPool customPool = new ForkJoinPool(10)) {
            customPool.submit( () -> StreamSupport.stream(new PassageSplitScan<>(context, new BackendBindings<>(), tp1), true)
                    .forEach((mu)-> StreamSupport.stream(new PassageSplitScan<>(context, mu, tp2), true)
                        .forEach((mu2)-> StreamSupport.stream(new PassageSplitScan<>(context, mu2, tp3), true)
                                .forEach(results::add))))
                    .join();
        }
        log.debug("{}", results);
        // enumerate all predicates `po` of alice for each animal: 4x3
        assertEquals(12, results.size());
        backend.close();
    }

}