package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.utils.MultisetResultChecking;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.transforms.DeduplicateFilters;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transformer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FilterTest {

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void simple_tp_filtered_by_one_var (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( ?address != <http://nantes> )
        }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size()); // Bob only
        assertTrue(MultisetResultChecking.containsResult(results, List.of("person", "address"),
                List.of("Bob", "paris")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void simple_tp_filtered_by_two_vars (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?address
            FILTER ( (?address != <http://nantes>) || (?person != <http://Alice>) )
        }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // Bob and Carol
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("person", "address"),
                List.of("Bob", "paris"),
                List.of("Carol", "nantes")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void filter_bgp_of_2_tps (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // Alice and Alice.
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "cat"),
                List.of("Alice", "snake")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void simple_bgp_filtered_in_the_middle (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> ?c .
                FILTER (?c != <http://nantes>)
                ?p <http://own> ?a
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size()); // No one that lives outside nantes has animals
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void simple_bgp_filtered_in_the_middle_but_different_order (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(blazegraph);
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://own> ?a
                FILTER (?a != <http://dog>)
                ?p <http://address> <http://nantes>
               }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // No one that lives outside nantes has animals
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("p", "a"),
                List.of("Alice", "snake"),
                List.of("Alice", "cat")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void filter_using_a_literal_integer (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT * WHERE {
                       ?animal <http://letters> ?number
                       FILTER (?number > 3)
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(1, results.size());
        // cat = 3 so filtered out
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal", "number"),
                List.of("snake", "5")));
        blazegraph.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pushProvider")
    public void filter_using_a_literal_and_a_function (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9PlusLiterals());
        builder.setBackend(blazegraph);
        String queryAsString = """
                SELECT ?animal WHERE {
                       ?person <http://own> ?animal
                       FILTER (strlen(str(?animal)) <= 8+3)
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(2, results.size()); // no snake this time
        assertTrue(MultisetResultChecking.containsAllResults(results, List.of("animal"),
                List.of("dog"),
                List.of("cat")));
        blazegraph.close();
    }

    @Disabled // TODO
    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneScanOneThreadOnePush")
    public void filter_should_not_duplicate (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        // Related to issue: https://github.com/passage-org/passage-comunica/issues/25
        // A filter is getting duplicated again and again, until there are a lot of copies
        // of the same filter; which, even though it's not semantically an issue, can have an impact on
        // query execution.
        final BlazegraphBackend bb = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(bb);
        String queryAsString =  """
                SELECT ?animal WHERE {
                       { SELECT * WHERE { ?person <http://own> ?animal }  OFFSET 0 }
                       UNION { SELECT * WHERE { ?person <http://own> ?animal }  OFFSET 0 }
                       {
                         ?person <http://own> ?animal
                       }
                       FILTER (strlen(str(?animal)) <= 8+3)
                }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        System.out.println(results);

        bb.close();
    }

    @Test
    public void workaround_to_remove_duplicated_filters () throws RepositoryException, SailException {
        String queryAsString = """
                SELECT ?animal WHERE {
                 ?s ?p ?animal .
                 FILTER (strlen(str(?animal)) <= 8+3)
                 FILTER (strlen(str(?animal)) <= 8+3)
                }
        """;

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Op queryDeduplicated = Transformer.transform(new DeduplicateFilters(), query);
        assertEquals(2, StringUtils.countMatches(OpAsQuery.asQuery(query).toString(), "strlen"));
        assertEquals(1, StringUtils.countMatches(OpAsQuery.asQuery(queryDeduplicated).toString(), "strlen"));

        // comes from: https://github.com/passage-org/passage-comunica/issues/25
        String queryAsStringFromIssue25 = """
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?article1 ?article2 WHERE {
                  {
                    {
                      {
                        BIND(wd:Q28206160 AS ?article1)
                        BIND(wd:Q74136123 AS ?article2)
                      }
                      ?article2 wdt:P31 wd:Q13442814.
                    }
                    ?article2 wdt:P2860 ?article1.
                  }
                  UNION
                  {
                    {
                      {
                        SELECT * WHERE {
                          BIND(wd:Q28206160 AS ?article1)
                          ?article1 wdt:P2860 ?article2.
                        }
                        OFFSET 31
                      }
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                      FILTER(?article1 != ?article2)
                    }
                    UNION
                    {
                      {
                        SELECT * WHERE { ?article1 wdt:P31 wd:Q13442814. }
                        OFFSET 372873
                      }
                      {
                        ?article1 wdt:P2860 ?article2.
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                        FILTER(?article1 != ?article2)
                      }
                    }
                    {
                      ?article2 wdt:P31 wd:Q13442814;
                        wdt:P2860 ?article1.
                    }
                  }
                }
                """;

        Op queryFromIssue25 = Algebra.compile(QueryFactory.create(queryAsStringFromIssue25));
        Op queryFromIssue25Deduplicated = Transformer.transform(new DeduplicateFilters(), queryFromIssue25);
        // the new query should be much smaller
        assertTrue(OpAsQuery.asQuery(queryFromIssue25).toString().length() >
                OpAsQuery.asQuery(queryFromIssue25Deduplicated).toString().length());
    }

}
