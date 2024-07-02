package fr.gdd.sage.rawer;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class RawerWatdivTest {

    private final static Logger log = LoggerFactory.getLogger(RawerWatdivTest.class);
    static BlazegraphBackend watdivBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl");

    @Disabled
    @Test
    public void count_star_on_spo () {
        String queryAsString = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, watdivBlazegraph, 1L); // 10,916,457 triples
    }

    @Disabled
    @Test
    public void count_s_on_spo () {
        // TODO TODO TODO variable bound in COUNT
        // TODO same for p, and o
        String queryAsString = "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, watdivBlazegraph, 1L);
    }

    @Disabled
    @Test
    public void count_distinct_s_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?s ) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, watdivBlazegraph, 1000000L); // 521,585 triples (+blaze default ones)
    }

    @Disabled
    @Test
    public void count_distinct_p_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?p ) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, watdivBlazegraph, 1000000L); // 86 triples (+blaze default ones)
    }

    @Disabled
    @Test
    public void count_distinct_o_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?o ) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, watdivBlazegraph, 1000000L); // 1,005,832 triples (+blaze default ones)
    }

    @Disabled
    @Test
    public void issue_with_blobs_not_being_blobs () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        String queryAsString = """
            SELECT * WHERE { ?s ?p "hooey porringers curved Coriolis floating rapid Hispaniola's rectifying averages militarization islander's nonaligned instigators obviated confrontational deathblow flank provoke lutes peroxide deerskin's shirrs unknown" }
            """;
        long nbResults = watdivBlazegraph.countQuery(queryAsString);
        assertTrue(nbResults > 0);
    }

}
