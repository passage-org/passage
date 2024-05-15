package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.resume.BGP2Triples;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.store.NodeId;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

@Disabled
class Save2SPARQLTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLTest.class);

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> String executeQuery(String queryAsString, Backend<ID, VALUE, ?> backend) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, backend);
        ec.getContext().set(SagerConstants.LIMIT, 1);

        SagerOpExecutor<ID, VALUE> executor = new SagerOpExecutor<>(ec);

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(query);
        if (!iterator.hasNext()) {
            return null;
        }
        log.debug("{}", iterator.next());

        Save2SPARQL<ID, VALUE> saver = ec.getContext().get(SagerConstants.SAVER);
        Op saved = saver.save(null);
        String savedAsString = OpAsQuery.asQuery(saved).toString();
        log.debug(savedAsString);
        return savedAsString;
    }

}