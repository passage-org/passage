package fr.gdd.passage.volcano.executions;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.benchmarks.WDBenchTest;
import fr.gdd.passage.volcano.push.streams.SpliteratorScan;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnionTest {

    private final static Logger log = LoggerFactory.getLogger(UnionTest.class);

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void execute_a_simple_union (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?a}
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        assertEquals(6, results.size()); // 3 triples + 3 triples
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "snake"), List.of("Alice", "dog"), List.of("Alice", "cat"),
                List.of("Alice", "nantes"), List.of("Bob", "paris"), List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void union_with_a_bgp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?address .
                 ?p  <http://own> ?a }
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        assertEquals(6, results.size()); // Alice x6
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider") // TODO issue on parallelism
    public void create_an_simple_union_with_bgp_inside(PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                {?p <http://own> ?a . ?a <http://species> ?s } UNION { ?p <http://address> <http://nantes> }
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        assertEquals(5, results.size()); // Alice * 3 + Alice + Carol
        blazegraph.close();
    }


    /* *************************** BIG DATASETS ******************************** */

    @Disabled // TODO put this in test dedicated to benchmarking units
    @Test
    public void query_358 () {
        final BlazegraphBackend blazegraph = WDBenchTest.wdbench;

//        String query358AsString = """
//                SELECT * WHERE {
//                  ?x1 <http://www.wikidata.org/prop/direct/P17> ?x2 .
//                  ?x3 <http://www.wikidata.org/prop/direct/P17> ?x2 .
//                  ?x1 <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q1194951> .
//                  ?x3 <http://www.wikidata.org/prop/direct/P31> <http://www.wikidata.org/entity/Q2485448> .
//                  ?x1 <http://www.wikidata.org/prop/direct/P641> <http://www.wikidata.org/entity/Q847> .
//                  ?x3 <http://www.wikidata.org/prop/direct/P641> <http://www.wikidata.org/entity/Q847> .
//                }""";

        String query646 = """
                SELECT * WHERE {
                  ?x1 <http://www.wikidata.org/prop/direct/P4614> ?x3 . #tp2
                  ?x1 <http://www.wikidata.org/prop/direct/P361> ?x2 .  #tp1
                  ?x2 <http://www.wikidata.org/prop/direct/P4614> ?x4 . #tp3
                  ?x4 <http://www.wikidata.org/prop/direct/P4614> ?x3 . #tp4
                }""";

        // var results = OpExecutorUtils.executeWithPush(query646, blazegraph);
        //log.debug("{}", results);
    }

    @Test
    public void query_watdiv () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Path.of("C:\\Users\\brice\\Downloads\\watdiv10m-blaze\\watdiv10M.jnl").toFile().exists());
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("C:\\Users\\brice\\Downloads\\watdiv10m-blaze\\watdiv10M.jnl");

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

        // var results = OpExecutorUtils.executeWithPush(query0, watdivBlazegraph);

        // assertEquals(117, results.size());
    }

    @Test
    public void query_watdiv_hand_written () throws RepositoryException, SailException {
        Assumptions.assumeTrue(Path.of("C:\\Users\\brice\\Downloads\\watdiv10m-blaze\\watdiv10M.jnl").toFile().exists());
        BlazegraphBackend backend = new BlazegraphBackend("C:\\Users\\brice\\Downloads\\watdiv10m-blaze\\watdiv10M.jnl");
        var c = new PassageExecutionContextBuilder().setBackend(backend).build();

        // SELECT  * WHERE {
        //    ?v1 <http://www.geonames.org/ontology#parentCountry>  ?v2 .
        //    ?v3 <http://purl.org/ontology/mo/performed_in>  ?v1
        //    ?v0 <http://purl.org/dc/terms/Location>  ?v1
        //    ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender>  <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1>
        //    ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId>  ?v5
        //    ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows>  ?v0
        // }

        Node p1 = NodeFactory.createURI("http://www.geonames.org/ontology#parentCountry");
        Node p2 = NodeFactory.createURI("http://purl.org/ontology/mo/performed_in");
        Node p3 = NodeFactory.createURI("http://purl.org/dc/terms/Location");
        Node p4 = NodeFactory.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/gender");
        Node p5 = NodeFactory.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/userId");
        Node p6 = NodeFactory.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/follows");

        Node o4 = NodeFactory.createURI("http://db.uwaterloo.ca/~galuc/wsdbm/Gender1");

        OpTriple tp1 = new OpTriple(Triple.create( Var.alloc("v1"), p1, Var.alloc("v2")));
        OpTriple tp2 = new OpTriple(Triple.create( Var.alloc("v3"), p2, Var.alloc("v1")));
        OpTriple tp3 = new OpTriple(Triple.create( Var.alloc("v0"), p3, Var.alloc("v1")));
        OpTriple tp4 = new OpTriple(Triple.create( Var.alloc("v0"), p4, o4));
        OpTriple tp5 = new OpTriple(Triple.create( Var.alloc("v0"), p5, Var.alloc("v5")));
        OpTriple tp6 = new OpTriple(Triple.create( Var.alloc("v0"), p6, Var.alloc("v0")));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        try (ForkJoinPool customPool = new ForkJoinPool(10)) {
            customPool.submit( () ->
                            StreamSupport.stream(new SpliteratorScan<>(meow(c), new BackendBindings<>(), tp1), true)
                                    .forEach((mu1)-> StreamSupport.stream(new SpliteratorScan<>(meow(c), mu1, tp2), true)
                                            .forEach((mu2)-> StreamSupport.stream(new SpliteratorScan<>(meow(c), mu2, tp3), true)
                                                    .forEach((mu3)-> StreamSupport.stream(new SpliteratorScan<>(meow(c), mu3, tp4), true)
                                                            .forEach((mu4)-> StreamSupport.stream(new SpliteratorScan<>(meow(c), mu4, tp5), true)
                                                                    .forEach((mu5)-> StreamSupport.stream(new SpliteratorScan<>(meow(c), mu5, tp6), true)
                                                                            .forEach(results::add)))))))
                    .join();
        }
        log.debug("{}", results.size());

        assertEquals(117, results.size());
    }


    static PassageExecutionContext<Object, Object> meow(ExecutionContext context) {
        return new PassageExecutionContextBuilder<>().setContext(context).build().setLimit(null).setOffset(0L);
    }
}
