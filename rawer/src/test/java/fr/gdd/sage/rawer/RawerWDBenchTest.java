package fr.gdd.sage.rawer;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled
public class RawerWDBenchTest {

    private final static Logger log = LoggerFactory.getLogger(RawerWDBenchTest.class);
    static BlazegraphBackend wdbenchBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl");

    @Disabled
    @Test
    public void count_star_on_spo () {
        String queryAsString = "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 1L); // 1,257,169,959 triples (+blaze's ones)
    }

    @Disabled
    @Test
    public void count_s_on_spo () {
        // TODO TODO TODO variable bound in COUNT
        // TODO same for p, and o
        String queryAsString = "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 1L);
    }

    @Disabled
    @Test
    public void count_distinct_s_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?s ) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 1000000L); // 92,498,623 (+blaze default ones)
    }

    @Disabled
    @Test
    public void count_distinct_p_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?p ) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 1000000L); // 8,604 (+blaze default ones)
    }

    @Disabled
    @Test
    public void count_distinct_o_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?o ) AS ?count) WHERE { ?s ?p ?o }";
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 1000000L); // 304,967,140 (+blaze default ones)
    }

    @Disabled
    @Test
    public void weird_object_not_getting_parsed () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        String queryAsString = """
            SELECT * WHERE { ?s ?p "IT\\ICCU\\CFIV\\072994" }
            """;
        wdbenchBlazegraph.executeQuery(queryAsString);
    }


}
