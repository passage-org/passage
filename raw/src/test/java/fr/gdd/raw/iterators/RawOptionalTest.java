package fr.gdd.raw.iterators;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.raw.RawOpExecutorUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class RawOptionalTest {

    private static final Logger log = LoggerFactory.getLogger(RandomQuadTest.class);
    static BlazegraphBackend dbpediaBlazegraph;
    static {
        try {
            dbpediaBlazegraph = new BlazegraphBackend("/Users/e23e889b/Documents/2025_03/dbpedia2021_09.jnl");
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }
    private void assertProbabilityPresent(Multiset<BackendBindings<?, ?>> results) {
        for (BackendBindings<?, ?> binding : results) {
            assertTrue(binding.getAllBindings().contains("probabilityOfRetrievingRestOfMapping"),
                    "Probability is missing in one of the results: " + binding);
        }
    }
    @Test
    public void SimpleOptional() {

        String Opquery = "SELECT ?p ?o ?p1 ?o1 ?probabilityOfRetrievingRestOfMapping WHERE { <http://dbpedia.org/resource/Vancouver> ?p ?o OPTIONAL { ?o ?p1 ?o1 } }";
        var results = RawOpExecutorUtils.executeWithRaw(Opquery, dbpediaBlazegraph, 1000L);
        log.debug("{}", results);
        log.debug("{}", results.size());
        assertTrue(results.size() > 0);

    }

    private BlazegraphBackend backend;

    @BeforeEach
    public void setUp() throws RepositoryException {
        backend = new BlazegraphBackend(IM4Blazegraph.triples6());
    }

    @Test
    public void SimpleOptional2() {
        String query = """
            PREFIX ex: <http://>
            SELECT ?person ?address ?pet ?probabilityOfRetrievingRestOfMapping WHERE {
                ?person ex:address ?address .
                OPTIONAL { ?person ex:own ?pet }
            }
            """;

        // Execute the query
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(query, backend, 10L);

        // Define the expected results as a Set
        Set<String> expectedResults = new HashSet<>(Set.of(
                "{?address-> <http://nantes> ; ?person-> <http://Alice> ; ?pet-> <http://cat> ; }",
                "{?address-> <http://nantes> ; ?person-> <http://Alice> ; ?pet-> <http://dog> ; }",
                "{?address-> <http://nantes> ; ?person-> <http://Alice> ; ?pet-> <http://snake> ; }",
                "{?address-> <http://paris> ; ?person-> <http://Bob> ; }",
                "{?address-> <http://nantes> ; ?person-> <http://Carol> ; }"
        ));
        log.debug("{}", results);
        Set<String> actualResults = results.stream()
                .map(Object::toString)
                .collect(Collectors.toSet());

        // Verify the results
        assertTrue(expectedResults.containsAll(actualResults),
                "Not all actual results are present in the expected results.");
        assertProbabilityPresent(results);
    }

    @Test
    public void tp_with_optional_tp () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT ?person ?address ?animal ?probabilityOfRetrievingRestOfMapping WHERE {
                ?person <http://address> ?address .
                OPTIONAL {?person <http://own> ?animal}
               }""";

        log.debug(queryAsString);
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, blazegraph, 10L);

        log.debug("{}", results);
         // (Alice + animal) * 3 + Bob + Carol
        // Define the expected results
        Multiset<BackendBindings<?,?>> expectedResults = HashMultiset.create();
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://cat>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://dog>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://snake>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Bob>", "address", "<http://paris>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Carol>", "address", "<http://nantes>")));

        // Verify the results
        assertTrue(expectedResults.containsAll(results),
                "Not all actual results are present in the expected results.");
    }
    //TODO: how to check if the actual results is bigger/smaller than the expected results( one can have duplicates but other may be missing)

    @Test
    public void tp_with_optional_tp_reverse_order () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT ?person ?animal ?probabilityOfRetrievingRestOfMapping WHERE {
                ?person <http://own> ?animal .
                OPTIONAL {?person <http://address> <http://nantes>}
               }""";
        log.debug(queryAsString);
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, blazegraph, 10L);
        log.debug("{}", results);

        // Define the expected results
        Multiset<BackendBindings<?,?>> expectedResults = HashMultiset.create();
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "animal", "<http://cat>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "animal", "<http://dog>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "animal", "<http://snake>")));

        // Verify the results
        assertTrue(expectedResults.containsAll(results),
                "Not all actual results are present in the expected results.");
    }

    @Test
    public void bgp_of_3_tps_and_optional () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT ?person ?address ?animal ?species ?probabilityOfRetrievingRestOfMapping WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   ?animal <http://species> ?specie
                 }
               }""";

        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, blazegraph, 10L);
        log.debug("{}", results);
        // (Alice + animal) * 3 + Bob + Carol
        // Define the expected results
        Multiset<BackendBindings<?,?>> expectedResults = HashMultiset.create();
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://cat>", "specie", "<http://feline>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://dog>", "specie", "<http://canine>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://snake>", "specie", "<http://reptile>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Bob>", "address", "<http://paris>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Carol>", "address", "<http://nantes>")));

        // Verify the results
        assertTrue(expectedResults.containsAll(results),
                "Not all actual results are present in the expected results.");
    }

    @Test
    public void bgp_of_3_tps_and_optional_of_optional () throws RepositoryException {
        BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
               SELECT ?person ?address ?animal ?species ?probabilityOfRetrievingRestOfMapping WHERE {
                 ?person <http://address> ?address .
                 OPTIONAL {
                   ?person <http://own> ?animal.
                   OPTIONAL {?animal <http://species> ?specie}
                 }
               }""";
        Multiset<BackendBindings<?,?>> results = RawOpExecutorUtils.executeWithRaw(queryAsString, blazegraph, 10L);
        log.debug("{}", results);
        // (Alice + animal) * 3 + Bob + Carol
        // Define the expected results
        Multiset<BackendBindings<?,?>> expectedResults = HashMultiset.create();
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://cat>", "specie", "<http://feline>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://dog>", "specie", "<http://canine>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Alice>", "address", "<http://nantes>", "animal", "<http://snake>", "specie", "<http://reptile>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Bob>", "address", "<http://paris>")));
        expectedResults.add(new BackendBindings<>(Map.of("person", "<http://Carol>", "address", "<http://nantes>")));

        // Verify the results
        assertTrue(expectedResults.containsAll(results),
                "Not all actual results are present in the expected results.");
    }
    @Test
public void test_literal_output() throws JSONException {
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