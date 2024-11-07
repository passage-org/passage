package fr.gdd.passage.volcano.pause;

import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Disabled
public class PauseUtils4Test {

    private static final Logger log = LoggerFactory.getLogger(PauseUtils4Test.class);

    // just a sample of stopping conditions based on scans
    public static final Function<ExecutionContext, Boolean> stopAtEveryScan = (ec) -> ((AtomicLong) ec.getContext().get(PassageConstants.SCANS)).get() >= 1;
    public static final Function<ExecutionContext, Boolean> stopEveryThreeScans = (ec) -> ((AtomicLong) ec.getContext().get(PassageConstants.SCANS)).get() >= 3;
    public static final Function<ExecutionContext, Boolean> stopEveryFiveScans = (ec) -> ((AtomicLong) ec.getContext().get(PassageConstants.SCANS)).get() >= 5;

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @return The preempted query after one result.
     */
    public static <ID,VALUE> Pair<Integer, String> executeQuery(String queryAsString, Backend<ID,VALUE,Long> backend) {
        return executeQuery(queryAsString, backend, 1L);
    }

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param backend The backend to execute on.
     * @param limit The number of actual results mappings before pausing.
     * @return The preempted query after one result.
     */
    public static <ID, VALUE> Pair<Integer, String> executeQuery(String queryAsString, Backend<ID, VALUE, Long> backend, Long limit) {
        PassageOpExecutor<ID, VALUE> executor = new PassageOpExecutor<>(
                new PassageExecutionContextBuilder<ID,VALUE>()
                        .setBackend(backend)
                        .build().setMaxResults(limit)); // As if it was within the query…

        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(queryAsString);
        int nbResults = 0;
        while (iterator.hasNext()) {
            log.debug("{}", iterator.next());
            nbResults += 1;
        };

        return new ImmutablePair<>(nbResults, executor.pauseAsString());
    }

    public static <ID, VALUE> Pair<Integer, String> executeQueryWithTimeout(String queryAsString, Backend<ID, VALUE, Long> backend, Long timeout) {
        PassageOpExecutor<ID, VALUE> executor = new PassageOpExecutor<>(
                new PassageExecutionContextBuilder<ID,VALUE>()
                        .setBackend(backend)
                        .setTimeout(timeout)
                        .build());
        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(queryAsString);

        int nbResults = 0;
        while (iterator.hasNext()){
            log.debug("{}", iterator.next());
            ++nbResults;
        }

        return new ImmutablePair<>(nbResults, executor.pauseAsString());
    }


    public static <ID,VALUE> String executeQuery(String queryAsString, Backend<ID,VALUE,Long> backend, Multiset<BackendBindings<?,?>> results) {
        PassageOpExecutor<ID, VALUE> executor = new PassageOpExecutor<>(
                new PassageExecutionContextBuilder<ID,VALUE>()
                        .setBackend(backend)
                        .build());
        Iterator<BackendBindings<ID, VALUE>> iterator = executor.execute(queryAsString);

        while (iterator.hasNext()){
            BackendBindings<ID,VALUE> next = iterator.next();
            results.add(next);
            log.debug("{}", next);
        }

        return executor.pauseAsString();
    }

}