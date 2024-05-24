package fr.gdd.sage.sager.benchmarks;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.sager.pause.Save2SPARQLTest;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class WatDivTest {

    private final static Logger log = LoggerFactory.getLogger(WatDivTest.class);
    static BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv_blazegraph/watdiv.jnl");

    @Disabled
    @Test
    public void watdiv_with_1s_timeout () {
        List<Pair<String, String>> queries = Watdiv10M.getQueries("/Users/nedelec-b-2/Desktop/Projects/" + Watdiv10M.QUERIES_PATH, Watdiv10M.blacklist);

        for (Pair<String, String> nameAndQuery : queries) {
            String[] splitted = nameAndQuery.getLeft().split("/");
            String name = splitted[splitted.length-1];
            String query = nameAndQuery.getRight();
            log.info(name);

            int nbResults = 0;
            int nbPreempt = -1;
            long start = System.currentTimeMillis();
            while (Objects.nonNull(query)) {
                log.debug(query);
                var result = Save2SPARQLTest.executeQueryWithTimeout(query, watdivBlazegraph, 60000L); // 1s timeout
                nbResults += result.getLeft();
                query = result.getRight();
                nbPreempt += 1;
            }
            long elapsed = System.currentTimeMillis() - start;

            log.info("{} {} {} {}", name, nbPreempt, nbResults, elapsed);
        }
    }

}
