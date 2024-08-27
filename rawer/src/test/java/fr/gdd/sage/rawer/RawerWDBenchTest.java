package fr.gdd.sage.rawer;

import com.google.common.collect.Multiset;
import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.rawer.iterators.RandomAggregator;
import org.apache.jena.atlas.lib.EscapeStr;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.lang.arq.ARQParser;
import org.apache.jena.sparql.lang.arq.ParseException;
import org.apache.jena.sparql.util.ExprUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@Disabled
public class RawerWDBenchTest {

    private final static Logger log = LoggerFactory.getLogger(RawerWDBenchTest.class);
    static BlazegraphBackend wdbenchBlazegraph;

    static {
        try {
            wdbenchBlazegraph = new BlazegraphBackend("/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl");
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

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
        RandomAggregator.SUBQUERY_LIMIT = 1;
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 1000000L); // 92,498,623 (+blaze default ones)
    }

    @Disabled
    @Test
    public void count_distinct_p_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?p ) AS ?count) WHERE { ?s ?p ?o }";
        RandomAggregator.SUBQUERY_LIMIT = 1;
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 1000000L); // 8,604 (+blaze default ones)
    }

    @Disabled
    @Test
    public void count_distinct_o_on_spo () {
        String queryAsString = "SELECT (COUNT( DISTINCT ?o ) AS ?count) WHERE { ?s ?p ?o }";
        RandomAggregator.SUBQUERY_LIMIT = 1;
        RawerOpExecutorTest.execute(queryAsString, wdbenchBlazegraph, 100000L); // 304,967,140 (+blaze default ones)
    }

    @Disabled
    @Test
    public void weird_object_not_getting_parsed () throws QueryEvaluationException, MalformedQueryException, RepositoryException {
        String queryAsString = """
            SELECT * WHERE { ?s ?p "IT\\\\ICCU\\\\CFIV\\\\072994" }
            """;
        var results = wdbenchBlazegraph.executeQuery(queryAsString);
        System.out.println(results);
    }

    @Disabled
    @Test
    public void jena_expr_parser_doesnot_parse_this_value () {
        String queryAsString = """
            SELECT * WHERE { ?s ?p "IT\\\\ICCU\\\\CFIV\\\\072994" }
            """;
        Op op = Algebra.compile(QueryFactory.create(queryAsString));
    }

    @Disabled
    @Test
    public void make_jena_understand_with_lang () throws ParseException {
        String queryAsString = """
              SELECT * WHERE {  ?s ?p "Results of surgical treatment in patients with \\"western\\" intrahepatic lithiasis."@en }
            """;
        Op op = Algebra.compile(QueryFactory.create(queryAsString));

        String lit = """
                "Results of surgical treatment in patients with \\"western\\" intrahepatic lithiasis."@en
                """;
        var parser = new ARQParser(new ByteArrayInputStream(lit.getBytes(StandardCharsets.UTF_8)));
        Node n = parser.RDFLiteral();
    }

    @Disabled
    @Test
    public void trying_to_format_horrible_strings () throws ParseException {
        String escaped = """
                IT\\ICCU\\CFIV\\072994"""; // one slash in reality
        escaped = EscapeStr.stringEsc(escaped);
        // escaped = EscapeStr.stringEsc(escaped);
        escaped = "\""+ escaped + "\"";
        System.out.println(escaped);

        var parser = new ARQParser(new ByteArrayInputStream(escaped.getBytes(StandardCharsets.UTF_8)));
        Node n = parser.RDFLiteral();
        System.out.println(n.toString());
    }



}
