package fr.gdd.passage.volcano.executions;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.volcano.ExecutorUtils;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.pull.iterators.PassageService;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Deprecated // WiP
@Disabled("WiP + find a way to simulate service endpoints through HTTP…")
public class ServiceTest {

    private final static Logger log = LoggerFactory.getLogger(ServiceTest.class);

    private static String endpoint(String address) {
        return "http://localhost:3000/fedshop.jnl/passage?default-graph-uri=" + address; }

    // TODO change this to put it inside the PassageContext
    @BeforeEach
    public void stop_after_every_service_call () { PassageService.stopping = (ec) ->
            ((AtomicLong) ec.getContext().get(PassageConstants.SERVICE_CALLS)).get() >= 1;
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider") // TODO pause on service calls
    public void fails_because_the_remote_service_does_not_exist (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        Assumptions.assumeFalse(endpointIsReachable("endpoint_that_does_not_exist", 80, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(useless);
        String queryAsString = """
                  SELECT * WHERE { SERVICE <http://endpoint_that_does_not_exist/> { {?s ?p ?o} } }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size());
        log.debug("{}", results);
        useless.close();
    }

    @Disabled("HttpExpception -1 to handle when make the query GET.")
    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider") // TODO pause on service calls
    public void fails_because_the_remote_service_port_is_not_answering (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, IOException, SailException {
        Assumptions.assumeFalse(endpointIsReachable("localhost", 9999, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(useless);
        String queryAsString = """
                  SELECT * WHERE { SERVICE <http://localhost:9999> { {?s ?p ?o} } }""";

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size());
        log.debug("{}", results);
        useless.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider") // TODO pause on service calls
    public void fails_because_the_remote_service_sends_error (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        Assumptions.assumeTrue(endpointIsReachable("localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(useless);
        String queryAsString = String.format("""
                  SELECT * WHERE { SERVICE <%s> { {?s ?p ?o} } }""",
                endpoint("meow"));

        var results = ExecutorUtils.execute(queryAsString, builder);
        assertEquals(0, results.size());
        log.debug("{}", results);
        useless.close();
    }

    /* ******************************* ON BIG DATASET *********************************** */

    @ParameterizedTest
    // @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider") // TODO pause on service calls
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#oneThreadPush")
    public void a_simple_spo_on_a_remote_passage_endpoint (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        Assumptions.assumeTrue(endpointIsReachable("localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(useless);
        String queryAsString = String.format("""
                SELECT * WHERE { SERVICE <%s> { {?s ?p ?o} } }""",
                endpoint("http://www.vendor0.fr/"));

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("Number of results: {}", results.size());
        // TODO assertions
        useless.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider") // TODO pause on service calls
    public void an_spo_with_an_input_to_send (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        Assumptions.assumeTrue(endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(useless);
        String queryAsString = String.format("""
                SELECT * WHERE {
                    BIND (<http://www.vendor0.fr/ProductType916> AS ?s)
                    SERVICE <%s> { {?s ?p ?o} } }""",
                endpoint("http://www.vendor0.fr/"));

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("{}", results);
        log.debug("Number of results: {}", results.size());
        // TODO assertions
        useless.close();
    }

    @ParameterizedTest
    @MethodSource("fr.gdd.passage.volcano.InstanceProviderForTests#pullProvider") // TODO pause on service calls
    public void test_with_a_fedshop_rsa_query (PassageExecutionContextBuilder<?,?> builder) throws RepositoryException, SailException {
        Assumptions.assumeTrue(ServiceTest.endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        builder.setBackend(useless);
        String queryAsString = String.format("""
                SELECT ?product ?label WHERE
                  {   {   {   {   {   {   { SERVICE SILENT <%s>
                                              { { ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature2 .
                                                  ?localProductFeature2
                                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935> .
                                                  ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature1 .
                                                  ?localProductFeature1
                                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774> .
                                                  ?product  a  ?localProductType .
                                                  ?localProductType
                                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647> .
                                                  ?product  <http://www.w3.org/2000/01/rdf-schema#label>  ?label .
                                                  ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?value1
                                                }
                                                FILTER ( ?value1 > 744 )
                                              }
                                          } UNION { SERVICE SILENT <%s>
                                              { { ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature2 .
                                                  ?localProductFeature2
                                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935> .
                                                  ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature1 .
                                                  ?localProductFeature1
                                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774> .
                                                  ?product  a  ?localProductType .
                                                  ?localProductType
                                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647> .
                                                  ?product  <http://www.w3.org/2000/01/rdf-schema#label>  ?label .\s
                                                  ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?value1
                                                }
                                                FILTER ( ?value1 > 744 )
                                              }
                                          } } UNION { SERVICE SILENT <%s>
                                          { { ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature2 .
                                              ?localProductFeature2
                                                        <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935> .
                                              ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature1 .
                                              ?localProductFeature1
                                                        <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774> .
                                              ?product  a  ?localProductType .
                                              ?localProductType
                                                        <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647> .
                                              ?product  <http://www.w3.org/2000/01/rdf-schema#label>  ?label .
                                              ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?value1
                                            }
                                            FILTER ( ?value1 > 744 )
                                          }
                                      } } UNION { SERVICE SILENT <%s>
                                      { { ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature2 .
                                          ?localProductFeature2
                                                    <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935> .
                                          ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature1 .
                                          ?localProductFeature1
                                                    <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774> .
                                          ?product  a  ?localProductType .
                                          ?localProductType
                                                    <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647> .
                                          ?product  <http://www.w3.org/2000/01/rdf-schema#label>  ?label .
                                          ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?value1
                                        }
                                        FILTER ( ?value1 > 744 )
                                      }
                                  } } UNION { SERVICE SILENT <%s>
                                  { { ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature2 .
                                      ?localProductFeature2
                                                <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935> .
                                      ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature1 .
                                      ?localProductFeature1
                                                <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774> .
                                      ?product  a  ?localProductType .
                                      ?localProductType
                                                <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647> .
                                      ?product  <http://www.w3.org/2000/01/rdf-schema#label>  ?label .
                                      ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?value1
                                    }
                                    FILTER ( ?value1 > 744 )
                                  }
                              } } UNION { SERVICE SILENT <%s>
                              { { ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature2 .
                                  ?localProductFeature2
                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935> .
                                  ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature1 .
                                  ?localProductFeature1
                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774> .
                                  ?product  a  ?localProductType .
                                  ?localProductType
                                            <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647> .
                                  ?product  <http://www.w3.org/2000/01/rdf-schema#label>  ?label .
                                  ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?value1
                                }
                                FILTER ( ?value1 > 744 )
                              }
                          } } UNION { SERVICE SILENT <%s>
                          { { ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature2 .
                              ?localProductFeature2
                                        <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935> .
                              ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>  ?localProductFeature1 .
                              ?localProductFeature1
                                        <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774> .
                              ?product  a  ?localProductType .
                              ?localProductType
                                        <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647> .
                              ?product  <http://www.w3.org/2000/01/rdf-schema#label>  ?label .
                              ?product  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1>  ?value1
                            }
                            FILTER ( ?value1 > 744 )
                          } } }""",
                endpoint("http://www.ratingsite97.fr/"), endpoint("http://www.ratingsite55.fr/"),
                endpoint("http://www.vendor30.fr/"), endpoint("http://www.ratingsite32.fr/"),
                endpoint("http://www.ratingsite45.fr/"), endpoint("http://www.ratingsite57.fr/"),
                endpoint("http://www.ratingsite0.fr/"));

        var results = ExecutorUtils.execute(queryAsString, builder);
        log.debug("Number of results: {}", results.size());
        useless.close();
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
