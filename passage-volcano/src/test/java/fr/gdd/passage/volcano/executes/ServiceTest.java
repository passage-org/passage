package fr.gdd.passage.volcano.executes;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated // WiP
@Disabled("WiP")
public class ServiceTest {

    private final static Logger log = LoggerFactory.getLogger(ServiceTest.class);

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void a_simple_spo_on_a_remote_passage_endpoint () throws RepositoryException {
        BlazegraphBackend useless = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
                SELECT * WHERE {
                    SERVICE <http://localhost:3000/fedshop.jnl/passage?default-graph-uri=http://www.vendor0.fr/> {
                        SELECT * WHERE {?s ?p ?o}
                    }
                }""";

        Multiset<BackendBindings<?,?>> results = OpExecutorUtils.executeWithPassage(queryAsString, useless);
        log.debug("{}", results);
    }


}
