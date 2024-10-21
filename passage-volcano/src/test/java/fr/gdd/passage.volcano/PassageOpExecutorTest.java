package fr.gdd.passage.volcano;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class PassageOpExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(PassageOpExecutorTest.class);

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, Backend<?,?,?> backend) {
        return executeWithPassage(queryAsString, new PassageExecutionContextBuilder().setBackend(backend).build());
    }

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, PassageExecutionContext ec) {
        PassageOpExecutor<?,?> executor = new PassageOpExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Iterator<? extends BackendBindings<?, ?>> iterator = executor.execute(query);

        int sum = 0;
        Multiset<BackendBindings<?,?>> bindings = HashMultiset.create();
        while (iterator.hasNext()) {
            BackendBindings<?,?> binding = iterator.next();
            bindings.add(binding);
            log.debug("{}: {}", sum, binding.toString());
            sum += 1;
        }
        return bindings;
    }

    /**
     * Checks if the set of results contains the values associated to the variables.
     * @param results
     * @param vars
     * @param values
     * @return
     */
    public static boolean containsResult(Multiset<BackendBindings<?,?>> results, List<String> vars, List<String> values) {
        return results.stream().anyMatch(
                result -> {
                    for (int i = 0; i < vars.size(); ++i) {
                        if (!result.get(Var.alloc(vars.get(i))).getString().contains(values.get(i))) {
                            return false;
                        }
                    }
                    return true;
                });
    }

    /* ****************************************************************** */

    @Disabled
    @Test
    public void on_watdiv_conjunctive_query_10124 () throws RepositoryException, SailException {
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

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

        int sum = executeWithPassage(query0, watdivBlazegraph).size();

        assertEquals(117, sum);
    }

    @Disabled
    @Test
    public void sandbox_of_test () throws RepositoryException, SailException {
        BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

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

        int sum = executeWithPassage(query, watdivBlazegraph).size();
        log.info("{}", sum);

    }

}