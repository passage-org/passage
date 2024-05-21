package fr.gdd.sage.sager;

import fr.gdd.sage.generics.BackendBindings;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

@Disabled
class SagerOpExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(SagerOpExecutorTest.class);

    public static int executeWithSager(String queryAsString, ExecutionContext ec) {
        ARQ.enableOptimizer(false); // to make sure jena does not do anything
        SagerOpExecutor<?,?> executor = new SagerOpExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Iterator<? extends BackendBindings<?, ?>> iterator = executor.optimizeThenExecute(query);

        int sum = 0;
        while (iterator.hasNext()) {
            BackendBindings<?,?> binding = iterator.next();
            log.debug("{}", binding.toString());
            sum += 1;
        }

        return sum;
    }

    /* ****************************************************************** */

    @Disabled
    @Test
    public void create_a_subquery_to_see_what_it_looks_like () {
        String queryAsString = """
            SELECT * WHERE {
                ?s <http://named> ?o . {
                    SELECT * WHERE {?o <http://owns> ?a} ORDER BY ?o OFFSET 1 LIMIT 1
                }}
            """;
        // Sub-queries are handled with JOIN of the inner operators of the query
        // always slice outside, then order, the bgp
        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        log.debug("{}", query);
    }
}