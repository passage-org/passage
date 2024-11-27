package fr.gdd.raw.subqueries;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.raw.executor.RawConstants;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Var;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CountSubqueryBuilderTest {

    private static final Logger log = LoggerFactory.getLogger(CountSubqueryBuilderTest.class);

    @Test
    public void small_rewriting_test_of_a_query_into_count () throws RepositoryException {
        BlazegraphBackend backend = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());

        String queryAsString = "SELECT * WHERE {?s <http://address> ?c . ?s <http://own> ?a}";
        String expectedAsString = String.format("""
                SELECT (COUNT(*) AS ?%s) WHERE {
                    <https://PLACEHOLDER_s> <http://address> ?c ;
                                     <http://own> ?a}
                """, RawConstants.COUNT_VARIABLE);
        Op expected = Algebra.compile(QueryFactory.create(expectedAsString));

        RawOpExecutor executor = new RawOpExecutor()
                .setBackend(backend);

        Iterator<BackendBindings> bindings = executor.execute(queryAsString);

        assertTrue(bindings.hasNext());
        BackendBindings binding = bindings.next(); // for sure there is

        // designed to replace the ?s with a placeholder :) in the COUNT query generated
        CountSubqueryBuilder builder = new CountSubqueryBuilder(executor.getBackend(),
                binding, Set.of(Var.alloc("s")));

        Op countSubquery = builder.build(queryAsString);
        log.debug("{}", OpAsQuery.asQuery(countSubquery));

        Query countSubqueryAsQuery = OpAsQuery.asQuery(countSubquery);
        assertEquals(OpAsQuery.asQuery(expected).toString(), countSubqueryAsQuery.toString());
    }

}