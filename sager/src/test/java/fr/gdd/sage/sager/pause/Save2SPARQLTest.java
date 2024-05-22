package fr.gdd.sage.sager.pause;

import fr.gdd.jena.visitors.ReturningOpVisitorRouter;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutor;
import fr.gdd.sage.sager.resume.BGP2Triples;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Function;

@Disabled
public class Save2SPARQLTest {

    private static final Logger log = LoggerFactory.getLogger(Save2SPARQLTest.class);

    static final Function<ExecutionContext, Boolean> stopAtEveryScan = (ec) -> {
        return ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 1; // stop at every scan
    };

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> Pair<Integer, String> executeQuery(String queryAsString, Backend<ID, VALUE, ?> backend) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, backend);
        ec.getContext().set(SagerConstants.LIMIT, 1);

        SagerOpExecutor<ID, VALUE> executor = new SagerOpExecutor<>(ec);

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(query);
        if (!iterator.hasNext()) {
            return new ImmutablePair<>(0, executor.pauseAsString());
        }
        log.debug("{}", iterator.next());

        return new ImmutablePair<>(1, executor.pauseAsString());
    }

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> Triple<Integer, String, Double> executeQueryWithProgress(String queryAsString, Backend<ID, VALUE, ?> backend) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, backend);
        ec.getContext().set(SagerConstants.LIMIT, 1);

        SagerOpExecutor<ID, VALUE> executor = new SagerOpExecutor<>(ec);

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(query);
        if (!iterator.hasNext()) {
            return new ImmutableTriple<>(0, executor.pauseAsString(), executor.progress());
        }
        log.debug("{}", iterator.next());

        return new ImmutableTriple<>(1, executor.pauseAsString(), executor.progress());
    }

}