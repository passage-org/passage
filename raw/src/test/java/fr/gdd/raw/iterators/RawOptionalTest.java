package fr.gdd.raw.iterators;

import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.raw.RawOpExecutorUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RawOptionalTest {

    private static final Logger log = LoggerFactory.getLogger(RawOptionalTest.class);
    static BlazegraphBackend dbpediaBlazegraph;
    static {
        try {
            // TODO change this path, download if need be
            dbpediaBlazegraph = new BlazegraphBackend("/Users/e23e889b/Documents/2025_03/dbpedia2021_09.jnl");
        } catch (SailException | RepositoryException | RuntimeException e) {
            dbpediaBlazegraph = null;
        }
    }

    @Test
    public void simple_optional() {
        Assumptions.assumeTrue(Objects.nonNull(dbpediaBlazegraph));
        String optQuery = "SELECT ?p ?o ?p1 ?o1 ?probabilityOfRetrievingRestOfMapping WHERE { <http://dbpedia.org/resource/Vancouver> ?p ?o OPTIONAL { ?o ?p1 ?o1 } }";
        var results = RawOpExecutorUtils.executeWithRaw(optQuery, dbpediaBlazegraph, 1000L);
        log.debug("{}", results);
        log.debug("{}", results.size());
        assertFalse(results.isEmpty());
    }

    @Test
    public void tp_with_optional_tp () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT ?person ?address ?animal ?probabilityOfRetrievingRestOfMapping WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";

        log.debug(queryAsString);
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, bb, 100L);
        log.debug("{}", results);

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "address", "animal"),
                List.of("Alice", "nantes", "cat"),
                List.of("Alice", "nantes", "dog"),
                List.of("Alice", "nantes", "snake"),
                Arrays.asList("Bob", "paris", null ),
                Arrays.asList("Carol", "nantes", null)));
        bb.close();
    }

    @Test
    public void tp_with_optional_tp_reverse_order () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT ?person ?animal ?probabilityOfRetrievingRestOfMapping WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";
        log.debug(queryAsString);
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, bb, 100L);
        log.debug("{}", results);

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "animal"),
                List.of("Alice", "cat"),
                List.of("Alice", "dog"),
                List.of("Alice", "snake")));
        bb.close();
    }

    @Test
    public void bgp_of_3_tps_and_optional () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT ?person ?address ?animal ?specie ?probabilityOfRetrievingRestOfMapping WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

        log.debug(queryAsString);
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, bb, 1000L);
        log.debug("{}", results);

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "address", "animal", "specie"),
                List.of("Alice", "nantes", "cat", "feline"),
                List.of("Alice", "nantes", "dog", "canine"),
                List.of("Alice", "nantes", "snake", "reptile"),
                Arrays.asList("Bob", "paris", null, null),
                Arrays.asList("Carol", "nantes", null, null)));
        bb.close();
    }

    @Test
    public void bgp_of_3_tps_and_optional_of_optional () throws RepositoryException, SailException {
        BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT ?person ?address ?animal ?specie ?probabilityOfRetrievingRestOfMapping WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   OPTIONAL {?animal <http://species> ?specie}
                 }
               }""";

        log.debug(queryAsString);
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, bb, 100L);
        log.debug("{}", results);

        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "address", "animal", "specie"),
                List.of("Alice", "nantes", "cat", "feline"),
                List.of("Alice", "nantes", "dog", "canine"),
                List.of("Alice", "nantes", "snake", "reptile"),
                Arrays.asList("Bob", "paris", null, null),
                Arrays.asList("Carol", "nantes", null, null)));
        bb.close();
    }

    @Disabled // TODO an actual assertion
    @Test
    public void test_literal_output() throws JSONException {
        Assumptions.assumeTrue(Objects.nonNull(dbpediaBlazegraph));
        String Opquery = "SELECT ?p ?o ?p1 ?o1 ?probabilityOfRetrievingRestOfMapping WHERE { <http://dbpedia.org/resource/Vancouver> ?p ?o OPTIONAL { ?o ?p1 ?o1 } }";
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(Opquery, dbpediaBlazegraph, 1000L);
        log.debug("{}", results);
        JSONArray jsonResults = new JSONArray();
        for (BackendBindings<?,?> binding : results) {
            JSONObject bindingObj = new JSONObject();
            binding.forEach((var, value) -> {
                try {
                    bindingObj.put(var.toString(), value.toString());
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
            });
            jsonResults.put(bindingObj);
        }
        log.debug("{}", jsonResults);
    }



}