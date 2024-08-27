package fr.gdd.sage.sager.optimizers;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.databases.inmemory.IM4Blazegraph;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class CardinalityJoinOrderingTest {

    private static final Logger log = LoggerFactory.getLogger(CardinalityJoinOrderingTest.class);

    @Test
    public void a_single_triple_pattern_stays_this_way_ofc () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";
        Op original = Algebra.compile(QueryFactory.create(queryAsString));
        Op reordered = new CardinalityJoinOrdering<>(blazegraph).visit(original);
        assertTrue(original.equalTo(reordered, null));
    }

    @Test
    public void a_single_triple_pattern_with_project_stays_this_way_ofc () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT ?p WHERE {?p <http://address> ?c}";
        Op original = Algebra.compile(QueryFactory.create(queryAsString));
        Op reordered = new CardinalityJoinOrdering<>(blazegraph).visit(original);
        assertTrue(original.equalTo(reordered, null));
    }

    @Test
    public void two_triple_patterns_stay_sorted () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?p <http://address> <http://nantes> . # card 2
            ?p <http://own> ?a . # card 3
        }""";
        Op original = Algebra.compile(QueryFactory.create(queryAsString));
        var ordering = new CardinalityJoinOrdering<>(blazegraph);
        Op reordered = ordering.visit(original);
        assertTrue(original.equalTo(reordered, null));
        assertFalse(ordering.hasCartesianProduct());
    }

    @Test
    public void two_triple_patterns_are_inverted () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String expectedQueryAsString = """
        SELECT * WHERE {
            ?p <http://address> <http://nantes> . # card 2
            ?p <http://own> ?a . # card 3
        }""";

        String queryAsString = """
        SELECT * WHERE {
            ?p <http://own> ?a . # card 3
            ?p <http://address> <http://nantes> . # card 2
        }""";

        Op original = Algebra.compile(QueryFactory.create(queryAsString));
        var ordering = new CardinalityJoinOrdering<>(blazegraph);
        Op reordered = ordering.visit(original);
        assertTrue(reordered.equalTo(Algebra.compile(QueryFactory.create(expectedQueryAsString)), null));
        assertFalse(ordering.hasCartesianProduct());
    }

    @Test
    public void cartesian_product_still_takes_smaller_first () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String expectedQueryAsString = """
        SELECT * WHERE {
            ?person <http://address> <http://nantes> . # card 2
            ?animal <http://species> ?specie . # card 3
        }""";

        String queryAsString = """
        SELECT * WHERE {
            ?animal <http://species> ?specie . # card 3
            ?person <http://address> <http://nantes> . # card 2
        }""";

        Op original = Algebra.compile(QueryFactory.create(queryAsString));
        var ordering = new CardinalityJoinOrdering<>(blazegraph);
        Op reordered = ordering.visit(original);
        assertTrue(reordered.equalTo(Algebra.compile(QueryFactory.create(expectedQueryAsString)), null));
        assertTrue(ordering.hasCartesianProduct());
    }

    @Test
    public void with_optional_we_keep_it_there_for_now_but_still_want_to_know_if_cartesian_product () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?city . # card 3
            OPTIONAL {?person <http://own> ?animal} . # card 3
        }""";

        Op original = Algebra.compile(QueryFactory.create(queryAsString));
        var ordering = new CardinalityJoinOrdering<>(blazegraph);
        Op reordered = ordering.visit(original);
        assertTrue(reordered.equalTo(Algebra.compile(QueryFactory.create(queryAsString)), null));
        assertFalse(ordering.hasCartesianProduct());
    }

    @Test
    public void optional_with_cartesian_product () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = """
        SELECT * WHERE {
            ?person <http://address> ?city . # card 3
            OPTIONAL {?meow <http://own> ?animal} . # card 3 but no bounded variable
        }""";

        Op original = Algebra.compile(QueryFactory.create(queryAsString));
        var ordering = new CardinalityJoinOrdering<>(blazegraph);
        Op reordered = ordering.visit(original);
        assertTrue(reordered.equalTo(Algebra.compile(QueryFactory.create(queryAsString)), null));
        assertTrue(ordering.hasCartesianProduct());
    }

    // An issue comes from such queries of WDBench where there are no
    // Cartesian product per se… Although there could be… not sure what to
    // decide of this…
    // SELECT * WHERE {
    // ?x1 <http://www.wikidata.org/prop/direct/P2174> ?x2 .
    // ?x3 <http://www.wikidata.org/prop/direct/P625> ?x4 .
    // OPTIONAL { ?x1 <http://www.wikidata.org/prop/direct/P937> ?x3 . }   }

}