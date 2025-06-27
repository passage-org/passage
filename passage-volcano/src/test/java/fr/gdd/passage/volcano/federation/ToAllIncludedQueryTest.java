package fr.gdd.passage.volcano.federation;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled
class ToAllIncludedQueryTest {

    private final static Logger log = LoggerFactory.getLogger(ToAllIncludedQueryTest.class);

    @Test
    public void transform_a_simple_triple_pattern_to_include_its_source_selection () {
        Op query = Algebra.compile(QueryFactory.create("SELECT * WHERE { ?s ?p ?o }"));

        ToSourceAssignmentQuery tsaq = new ToSourceAssignmentQuery("http://meow");
        Op ss = tsaq.visit(query);
        log.debug("Source assignment query: {}", OpAsQuery.asQuery(ss));

        ToAllIncludedQuery taiq = new ToAllIncludedQuery("http://meow");
        Op allIncluded = taiq.visit(query);
        log.debug("{}", OpAsQuery.asQuery(allIncluded));
    }

    @Test
    public void a_triple_pattern_without_changes_but_not_spo () {
        Op query = Algebra.compile(QueryFactory.create("""
                SELECT * WHERE { ?s <http://predicate> ?o }
                """
        ));

        ToAllIncludedQuery taiq = new ToAllIncludedQuery("http://meow");
        Op allIncluded = taiq.visit(query);
        log.debug("{}", OpAsQuery.asQuery(allIncluded));
    }

    @Test
    public void a_triple_pattern_with_changes_since_there_is_a_constant () {
        Op query = Algebra.compile(QueryFactory.create("""
                SELECT * WHERE { ?s <http://predicate> <http://rating_site.fr/something/product12> }
                """
        ));

        ToAllIncludedQuery taiq = new ToAllIncludedQuery("http://meow");
        Op allIncluded = taiq.visit(query);
        log.debug("{}", OpAsQuery.asQuery(allIncluded));
    }

    @Test
    public void a_bgp_that_should_create_two_summary_queries () {
        // most importantly, the variable ?s must stay bounded when creating the summary
        // query.
        Op query = Algebra.compile(QueryFactory.create("""
                SELECT * WHERE {
                    ?s <http://predicate> <http://rating_site.fr/something/product12>.
                    ?s <http://has_review> ?review}
                """
        ));

        ToAllIncludedQuery taiq = new ToAllIncludedQuery("http://meow");
        Op allIncluded = taiq.visit(query);
        log.debug("{}", OpAsQuery.asQuery(allIncluded));
    }

    @Test
    public void a_simple_optional () {
        Op query = Algebra.compile(QueryFactory.create("""
                SELECT * WHERE {
                    ?s <http://predicate> <http://rating_site.fr/something/product12>.
                    OPTIONAL {?s <http://has_review> ?review}}
                """));

        ToAllIncludedQuery taiq = new ToAllIncludedQuery();
        Op allIncluded = taiq.create(query);
        log.debug("{}", OpAsQuery.asQuery(allIncluded));
    }

}