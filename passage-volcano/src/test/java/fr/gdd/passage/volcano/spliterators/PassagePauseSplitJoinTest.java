package fr.gdd.passage.volcano.spliterators;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.iterators.PassageScan;
import fr.gdd.passage.volcano.pause.PauseUtils4Test;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;

public class PassagePauseSplitJoinTest {

    private static final Logger log = LoggerFactory.getLogger(PassagePauseSplitJoinTest.class);

    @BeforeEach
    public void stop_every_scan() { PassageSplitScan.stopping =
            (ec) -> ((AtomicLong) ec.getContext().get(PassageConstants.SCANS)).get() >= 2; }

    @RepeatedTest(1)
    public void create_a_bgp_query_and_pause_at_each_result () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                ?p <http://address> <http://nantes> .
                ?p <http://own> ?a .
               }""";

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(new PassageExecutionContextBuilder().setBackend(blazegraph).build());

        executor.execute(query, results::add);
        log.debug("{}", results);
    }

}
