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
import fr.gdd.passage.volcano.benchmarks.WatDivTest;
import fr.gdd.passage.volcano.push.streams.SpliteratorScan;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

public class BGPTest {

    private final static Logger log = LoggerFactory.getLogger(BGPTest.class);

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void weird_string_in_query (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        String path2dbpedia = "/Users/nedelec-b-2/Desktop/Projects/temp/dbpedia-blaze/dbpedia2021_09.properties";
        Assumptions.assumeTrue(Path.of(path2dbpedia).toFile().exists());
        final BlazegraphBackend bb = new BlazegraphBackend(path2dbpedia);
        builder.setBackend(bb);
        String queryAsString = """
                SELECT * WHERE {?s ?p \"\\"By Sea, Land, and Air We Prosper\\"\"}
                """;
        var results = ExecutorUtils.execute(queryAsString, builder);
        assertFalse(results.isEmpty());
        bb.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void a_literal_at_predicate_position (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { "12" }
                    ?p ?predicate ?c
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size());
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void a_tp_with_an_unknown_value (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://does_not_exist> ?c}";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size());
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    public void a_bgp_with_an_unknown_value (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c . ?p <http://does_not_exist> ?c}";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size());
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    void an_unknown_value_at_first_but_then_known (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { <http://does_not_exist> <http://address> }
                    ?p ?predicate ?c}
                """;

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "predicate", "c"),
                List.of("Alice", "address", "nantes"),
                List.of("Bob", "address", "paris"),
                List.of("Carol", "address", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    void an_unknown_value_at_first_but_then_known_but_known_first (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                    VALUES ?predicate { <http://address> <http://does_not_exist> }
                    ?p ?predicate ?c}
                """;

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size());
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "predicate", "c"),
                List.of("Alice", "address", "nantes"),
                List.of("Bob", "address", "paris"),
                List.of("Carol", "address", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void bgp_of_1_tp (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Bob, Alice, and Carol.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "c"),
                List.of("Alice", "nantes"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void bgp_of_2_tps (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Alice, Alice, and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "dog"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void bgp_of_3_tps (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
                ?a <http://species> ?s
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(3, results.size()); // Alice->own->cat,dog,snake
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "dog", "canine"),
                List.of("Alice", "cat", "feline"),
                List.of("Alice", "snake", "reptile")));
        blazegraph.close();
    }


    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void issue_with_the_first_offset (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException, QueryEvaluationException, MalformedQueryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT  * WHERE {   { {
                  SELECT  * WHERE {
                    BIND(<http://Alice> AS ?p)
                    ?p  <http://own>  ?a
                 } OFFSET  1 } }
                 UNION
                 { { SELECT * WHERE { ?p  <http://address>  <http://nantes> } OFFSET  1 }
                        ?p  <http://own>  ?a }
                    ?a  <http://species>  ?s
                 }""";

        var expected = blazegraph.executeQuery(queryAsString);
        log.debug("{}", expected);

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // Alice->own->cat,dog,snake
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a", "s"),
                List.of("Alice", "dog", "canine"),
                // List.of("Alice", "cat", "feline"), cat is already produced
                List.of("Alice", "snake", "reptile")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider")
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void offset_triple_pattern_return_no_match (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);

        // Here, there is a second ?p from Nantes, but she does not own any animal

        String queryAsString = """
                SELECT * WHERE { { { SELECT  *  WHERE  { ?p  <http://address>  <http://nantes> }  OFFSET  1 }
                      ?p  <http://own>  ?a }
                    ?a  <http://species>  ?s }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size());
        blazegraph.close();
    }

    /* *************************** BIG DATASETS ******************************** */

    @Disabled // TODO put it in dedicated tests using benchmarks
    @Test
    public void testing_a_query_on_wdbench () {
        Assumptions.assumeTrue(Path.of(WDBenchTest.PATH).toFile().exists());
        final BlazegraphBackend wdbench = WDBenchTest.wdbench;

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

        // var results = OpExecutorUtils.executeWithPush(query646, wdbench);
        // log.debug("{}", results);
    }

    @Disabled // TODO put it in dedicated tests using benchmarks
    @RepeatedTest(5)
    public void query_watdiv_on_10124 () {
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdiv = WatDivTest.watdiv;

        String query10124 = """
                    SELECT * WHERE {
                            ?v1 <http://www.geonames.org/ontology#parentCountry> ?v2.
                            ?v3 <http://purl.org/ontology/mo/performed_in> ?v1.
                            ?v0 <http://purl.org/dc/terms/Location> ?v1.
                            ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1>.
                            ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/userId> ?v5.
                            ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/follows> ?v0.
                    }
                    """;

        // var results = OpExecutorUtils.executeWithPush(query10124, watdiv, 10);

        // assertEquals(117, results.size());
    }

    @Disabled // TODO put it in dedicated benchmarking tool
    @RepeatedTest(5)
    public void spo_on_watdiv () {
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdiv = WatDivTest.watdiv;
        String spo = "SELECT * WHERE {?s ?p ?o}";

        LongAdder results = new LongAdder();
        // OpExecutorUtils.execute(spo, watdiv, 10, result -> results.increment());
        // log.debug("Number of results: {}", results.longValue());
    }

    @Disabled // TODO put it in dedicated tests using benchmarks
    @Test
    public void query_watdiv_hand_written () {
        Assumptions.assumeTrue(Path.of(WatDivTest.PATH).toFile().exists());
        BlazegraphBackend watdiv = WatDivTest.watdiv;
        var c = new PassageExecutionContextBuilder<>().setBackend(watdiv).build();

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
                            StreamSupport.stream(new SpliteratorScan<>(reset(c), new BackendBindings<>(), tp1), true)
                                    .forEach((mu1)-> StreamSupport.stream(new SpliteratorScan<>(reset(c), mu1, tp2), true)
                                            .forEach((mu2)-> StreamSupport.stream(new SpliteratorScan<>(reset(c), mu2, tp3), true)
                                                    .forEach((mu3)-> StreamSupport.stream(new SpliteratorScan<>(reset(c), mu3, tp4), true)
                                                            .forEach((mu4)-> StreamSupport.stream(new SpliteratorScan<>(reset(c), mu4, tp5), true)
                                                                    .forEach((mu5)-> StreamSupport.stream(new SpliteratorScan<>(reset(c), mu5, tp6), true)
                                                                            .forEach(results::add)))))))
                    .join();
        }
        log.debug("{}", results.size());

        assertEquals(117, results.size());
    }

    static PassageExecutionContext<Object, Object> reset(ExecutionContext context) {
        return new PassageExecutionContextBuilder<>().setContext(context).build().setLimit(null).setOffset(0L);
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

        // int sum = OpExecutorUtils.execute(query, watdivBlazegraph).size();
        // log.info("{}", sum);
    }

}
