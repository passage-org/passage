package fr.gdd.passage.volcano.push.pause;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.spliterators.PassagePushExecutor;
import fr.gdd.passage.volcano.spliterators.PassageSplitScan;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class PassagePauseSplitUnionTest {

    private static final Logger log = LoggerFactory.getLogger(PassagePauseSplitUnionTest.class);

    @BeforeEach
    public void stop_every_scan() { PassageSplitScan.stopping =
            (ec) -> ((AtomicLong) ec.getContext().get(PassageConstants.SCANS)).get() >= 1; }

    @RepeatedTest(1)
    public void union_of_two_simple_triple_patterns () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?a}
               }""";

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(new PassageExecutionContextBuilder()
                .setBackend(blazegraph)
                .build());

        Op paused = executor.execute(query, results::add);
        log.debug("{}", results);
        if (Objects.nonNull(paused)) {
            log.debug("{}", OpAsQuery.asQuery(paused).toString());
        }
    }

    @RepeatedTest(1)
    public void union_with_a_bgp () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
               SELECT * WHERE {
                {?p  <http://own>  ?a}
                UNION
                {?p  <http://address> ?address .
                 ?p  <http://own> ?a }
               }""";

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(new PassageExecutionContextBuilder()
                .setBackend(blazegraph)
                .setMaxParallel(1)
                .build());

        Op paused = executor.execute(query, results::add);
        log.debug("{}", results);
        if (Objects.nonNull(paused)) {
            log.debug("{}", OpAsQuery.asQuery(paused).toString());
        }
    }

    @RepeatedTest(1)
    public void a_triple_pattern_that_is_split () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = "SELECT * WHERE { ?s ?p ?o }";

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(new PassageExecutionContextBuilder().setBackend(blazegraph).build());

        Op paused = executor.execute(query, results::add);
        log.debug("{}", results);
        log.debug("nb results= {}", results.size());
        if (Objects.nonNull(paused)) {
            log.debug("{}", OpAsQuery.asQuery(paused).toString());
        }
    }

}
