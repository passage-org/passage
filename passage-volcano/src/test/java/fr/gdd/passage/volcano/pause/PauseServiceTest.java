package fr.gdd.passage.volcano.pause;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.blazegraph.datasets.BlazegraphInMemoryDatasetsFactory;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.executes.ServiceTest;
import fr.gdd.passage.volcano.iterators.PassageService;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Deprecated // WiP
@Disabled("WiP + find a way to simulate service endpoints through HTTP…")
public class PauseServiceTest {

    private final static Logger log = LoggerFactory.getLogger(PauseServiceTest.class);

    private static String endpoint(String address) {
        return "http://localhost:3000/fedshop.jnl/passage?default-graph-uri=" + address;
    }

    @BeforeEach
    public void stop_after_every_service_call () { PassageService.stopping = (ec) ->
            ((AtomicLong) ec.getContext().get(PassageConstants.SERVICE_CALLS)).get() >= 1;
    }

    @Test
    public void a_simple_spo_on_a_remote_passage_endpoint () throws RepositoryException {
        Assumptions.assumeTrue(ServiceTest.endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = String.format("SELECT * WHERE { SERVICE <%s> { {?s ?p ?o} } }",
                endpoint("http://www.vendor0.fr/"));

        int nbContinuations = -1;
        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, useless, results);
            ++nbContinuations;
        }
        log.debug("Number of results: {}", results.size());
        log.debug("Number of continuation queries: {}", nbContinuations);
    }

    @Test
    public void an_spo_with_an_input_to_send () throws RepositoryException {
        Assumptions.assumeTrue(ServiceTest.endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
        String queryAsString = String.format("""
                SELECT * WHERE {
                    BIND (<http://www.vendor0.fr/ProductType916> AS ?s)
                    SERVICE <%s> { {?s ?p ?o} }
                }""", endpoint("http://www.vendor0.fr/"));

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, useless, results);
        }
        // of course there are but a few results (5), so it does not pause at all
        log.debug("Number of results: {}", results.size());
        assertEquals(0, results.size());
    }

    @Test
    public void test_with_a_fedshop_rsa_query () throws RepositoryException {
        Assumptions.assumeTrue(ServiceTest.endpointIsReachable("http://localhost", 3000, 2000));
        BlazegraphBackend useless = new BlazegraphBackend(BlazegraphInMemoryDatasetsFactory.triples9());
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

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            log.debug(queryAsString);
            queryAsString = PauseUtils4Test.executeQuery(queryAsString, useless, results);
        }
        log.debug("Number of results: {}", results.size());
    }





}
