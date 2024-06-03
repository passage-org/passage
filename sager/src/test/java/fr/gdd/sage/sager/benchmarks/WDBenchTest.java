package fr.gdd.sage.sager.benchmarks;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Disabled
public class WDBenchTest {

    private final static Logger log = LoggerFactory.getLogger(WDBenchTest.class);
    static BlazegraphBackend wdbenchBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/mail-about-ingesting-in-blazegraph/blazegraph_wdbench.jnl");

    @Disabled
    @Test
    public void execute_and_monitor_optionals_of_wdbench () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        List<Pair<String, String>> queries= Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench_opts", List.of());

        for (Pair<String, String> nameAndQuery: queries) {
            String[] splitted = nameAndQuery.getLeft().split("/");
            String name = splitted[splitted.length-1];
            String query = nameAndQuery.getRight();
            log.debug("Executing query {}…", name);
            log.debug(query);

            long start = System.currentTimeMillis();
            long nbResults = wdbenchBlazegraph.countQuery(query);
            long elapsed = System.currentTimeMillis() - start;

            log.info("{} {} {} {}", name, 0, nbResults, elapsed);
        }
    }

}
