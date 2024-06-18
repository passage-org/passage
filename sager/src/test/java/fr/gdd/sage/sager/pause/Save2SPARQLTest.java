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

    public static final Function<ExecutionContext, Boolean> stopAtEveryScan = (ec) -> ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 1;
    public static final Function<ExecutionContext, Boolean> stopEveryTwoScans = (ec) -> ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 2;
    public static final Function<ExecutionContext, Boolean> stopEveryThreeScans = (ec) -> ec.getContext().getLong(SagerConstants.SCANS, 0L) >= 3;

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> Pair<Integer, String> executeQuery(String queryAsString, Backend<ID, VALUE, ?> backend) {
        return executeQuery(queryAsString, backend, 1L);
    }

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @param limit The number of actual results mappings before pausing.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> Pair<Integer, String> executeQuery(String queryAsString, Backend<ID, VALUE, ?> backend, Long limit) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, backend);

        SagerOpExecutor<ID, VALUE> executor = new SagerOpExecutor<ID,VALUE>(ec).setLimit(limit);

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(query);
        int nbResults = 0;
        while (iterator.hasNext()) {
            log.debug("{}", iterator.next());
            nbResults += 1;
        };

        return new ImmutablePair<>(nbResults, executor.pauseAsString());
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

    public static <ID, VALUE> Pair<Integer, String> executeQueryWithTimeout(String queryAsString, Backend<ID, VALUE, ?> backend, Long timeout) {
        ARQ.enableOptimizer(false);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        query = ReturningOpVisitorRouter.visit(new BGP2Triples(), query);

        ExecutionContext ec = new ExecutionContext(DatasetFactory.empty().asDatasetGraph());
        ec.getContext().set(SagerConstants.BACKEND, backend);

        SagerOpExecutor<ID, VALUE> executor = new SagerOpExecutor<ID,VALUE>(ec).setTimeout(timeout);

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(query);

        int nbResults = 0;
        while (iterator.hasNext()){
            log.debug("{}", iterator.next());
            ++nbResults;
        }

        return new ImmutablePair<>(nbResults, executor.pauseAsString());
    }

}