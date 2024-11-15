package fr.gdd.passage.volcano.optimizers;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import fr.gdd.passage.volcano.transforms.BGP2Triples;
import fr.gdd.passage.volcano.transforms.PartitionRoot;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.RepositoryException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled(value = "WiP.")
public class PartitionRootTest {

    @Test
    public void basic_triple_pattern_creates_a_union () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));
        queryAsOp = ReturningOpVisitorRouter.visit(new BGP2Triples(), queryAsOp);
        PartitionRoot<?,?> pr = new PartitionRoot<>(blazegraph, 10);
        Op result = pr.visit(queryAsOp);

        String expectedAsString = String.format("""
                SELECT * WHERE {
                { { { %s LIMIT 1 OFFSET 0 } }
                  UNION { { { %s LIMIT 1 OFFSET 1 } }
                } UNION { { { %s OFFSET 2 } }
                }}}""", queryAsString, queryAsString, queryAsString);
        Op expectedAsOp = Algebra.compile(QueryFactory.create(expectedAsString));
        expectedAsOp = ReturningOpVisitorRouter.visit(new BGP2Triples(), expectedAsOp);
        assertEquals(expectedAsOp, result);
    }

    @Test
    public void triple_pattern_without_thread_available () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String queryAsString = "SELECT * WHERE {?p <http://address> ?c}";

        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));
        queryAsOp = ReturningOpVisitorRouter.visit(new BGP2Triples(), queryAsOp);
        PartitionRoot<?,?> pr = new PartitionRoot<>(blazegraph, 1);
        Op result = pr.visit(queryAsOp);

        assertEquals(queryAsOp, result); // no changes
    }

    @Test
    public void bgp_only_partition_the_root () throws RepositoryException {
        final BlazegraphBackend blazegraph = new BlazegraphBackend(IM4Blazegraph.triples9());
        String tp1 = "?p <http://address> ?c";
        String tp2 = "?p <http://own> ?a";

        String queryAsString = String.format("SELECT * WHERE { %s . %s }", tp1, tp2);
        String sqtp1 = String.format("SELECT * WHERE { %s }", tp1);

        Op queryAsOp = Algebra.compile(QueryFactory.create(queryAsString));
        queryAsOp = ReturningOpVisitorRouter.visit(new BGP2Triples(), queryAsOp);
        PartitionRoot<?,?> pr = new PartitionRoot<>(blazegraph, 10);
        Op result = pr.visit(queryAsOp);

        // The result should be the query copied 3 times since there are 3 mappings for the first
        // triple pattern.
        String expectedAsString = String.format("""
                SELECT * WHERE {
                { { { %s LIMIT 1 OFFSET 0 } %s }
                  UNION { { { %s LIMIT 1 OFFSET 1 } %s }
                } UNION { { { %s OFFSET 2 } %s }
                }}}""", sqtp1, tp2, sqtp1, tp2, sqtp1, tp2);

        Op expectedAsOp = Algebra.compile(QueryFactory.create(expectedAsString));
        expectedAsOp = ReturningOpVisitorRouter.visit(new BGP2Triples(), expectedAsOp);

        assertEquals(expectedAsOp, result);
    }


}