package fr.gdd.passage.volcano.pull.execute;

import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.OpExecutorUtils;
import fr.gdd.passage.volcano.iterators.PassageScan;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Deprecated // WiP
@Disabled("WiP + find a way to simulate service endpoints through HTTP…")
public class ServiceTest {

    private final static Logger log = LoggerFactory.getLogger(ServiceTest.class);

    private static String endpoint(String address) {
        return "http://localhost:3000/fedshop.jnl/passage?default-graph-uri=" + address; }

    @BeforeEach
    public void make_sure_we_dont_stop () { PassageScan.stopping = (e) -> false; }

    @Test
    public void fails_because_the_remote_service_does_not_exist() throws RepositoryException {
        Assumptions.assumeFalse(endpointIsReachable("http://endpoint_that_does_not_exist/", 80, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                  SELECT * WHERE { SERVICE <http://endpoint_that_does_not_exist/> { {?s ?p ?o} } }""";

        Multiset<BackendBindings<?,?>> results = OpExecutorUtils.executeWithPassage(queryAsString, useless);
        assertEquals(0, results.size());
        log.debug("{}", results);
    }

    @Disabled("HttpExpception -1 to handle when make the query GET.")
    @Test
    public void fails_because_the_remote_service_port_is_not_answering() throws RepositoryException, IOException {
        Assumptions.assumeFalse(endpointIsReachable("http://localhost", 9999, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = """
                  SELECT * WHERE { SERVICE <http://localhost:9999> { {?s ?p ?o} } }""";

        Multiset<BackendBindings<?,?>> results = OpExecutorUtils.executeWithPassage(queryAsString, useless);
        assertEquals(0, results.size());
        log.debug("{}", results);
    }

    @Test
    public void fails_because_the_remote_service_sends_error() throws RepositoryException {
        Assumptions.assumeTrue(endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = String.format("""
                  SELECT * WHERE { SERVICE <%s> { {?s ?p ?o} } }""",
                endpoint("meow"));

        Multiset<BackendBindings<?,?>> results = OpExecutorUtils.executeWithPassage(queryAsString, useless);
        assertEquals(0, results.size());
        log.debug("{}", results);
    }

    /* ******************************* ON BIG DATASET *********************************** */

    @Test
    public void a_simple_spo_on_a_remote_passage_endpoint () throws RepositoryException {
        Assumptions.assumeTrue(endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = String.format("""
                SELECT * WHERE { SERVICE <%s> { {?s ?p ?o} } }""",
                endpoint("http://www.vendor0.fr/"));

        Multiset<BackendBindings<?,?>> results = OpExecutorUtils.executeWithPassage(queryAsString, useless);
        log.debug("Number of results: {}", results.size());
    }

    @Test
    public void an_spo_with_an_input_to_send () throws RepositoryException {
        Assumptions.assumeTrue(endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = String.format("""
                SELECT * WHERE {
                    BIND (<http://www.vendor0.fr/ProductType916> AS ?s)
                    SERVICE <%s> { {?s ?p ?o} } }""",
                endpoint("http://www.vendor0.fr/"));

        Multiset<BackendBindings<?,?>> results = OpExecutorUtils.executeWithPassage(queryAsString, useless);
        log.debug("{}", results);
        log.debug("Number of results: {}", results.size());
    }

    /* ******************************** UTILS ******************************** */

    /**
     * @param host The endpoint address.
     * @param port The port that it listen to.
     * @param timeout The timeout before realizing it's not reachable (in milliseconds)
     * @return True if the endpoint is reachable, false otherwise.
     */
    public static boolean endpointIsReachable(String host, int port, int timeout) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeout);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

}
