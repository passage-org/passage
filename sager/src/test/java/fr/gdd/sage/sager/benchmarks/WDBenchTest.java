package fr.gdd.sage.sager.benchmarks;

import com.bigdata.concurrent.TimeoutException;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.sager.optimizers.CardinalityJoinOrdering;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Disabled
public class WDBenchTest {

    private final static Logger log = LoggerFactory.getLogger(WDBenchTest.class);
    static BlazegraphBackend wdbenchBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/mail-about-ingesting-in-blazegraph/blazegraph_wdbench.jnl");
    static final Long TIMEOUT = 1000L * 60L * 2L; // 2 minutes

    @Disabled
    @Test
    public void execute_and_monitor_optionals_of_wdbench () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench_opts", List.of());

        List<Pair<String, String>> filtered = new ArrayList<>();
        for (Pair<String, String> nameAndQuery : queries) {
            Op original = Algebra.compile(QueryFactory.create(nameAndQuery.getRight()));
            CardinalityJoinOrdering cjo = new CardinalityJoinOrdering(wdbenchBlazegraph);
            Op reordered = cjo.visit(original);
            if (cjo.hasCartesianProduct()) {
                log.debug("{} filtered out.", nameAndQuery.getLeft());
                log.debug(nameAndQuery.getRight());
            } else {
                filtered.add(nameAndQuery);
            }
        }

        log.info("Kept {} queries out of {}.", filtered.size(), queries.size());
        queries = filtered;

        for (Pair<String, String> nameAndQuery: queries) {
            String[] splitted = nameAndQuery.getLeft().split("/");
            String name = splitted[splitted.length-1];
            String query = nameAndQuery.getRight();
            log.debug("Executing query {}…", name);
            log.debug(query);

            long start = System.currentTimeMillis();
            long nbResults = -1;
            boolean timedout = false;
            try {
                nbResults = wdbenchBlazegraph.countQuery(query, TIMEOUT);
            } catch (TimeoutException e) {
                timedout = true;
                nbResults = Long.parseLong(e.getMessage());
            }
            long elapsed = System.currentTimeMillis() - start;

            log.info("{} {} {} {} {}", name, 0, nbResults, elapsed, timedout);
     }
    }

}
