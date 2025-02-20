package fr.gdd.raw;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.raw.executor.RawOpExecutor;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class RawOpExecutorUtils {

    private static final Logger log = LoggerFactory.getLogger(RawOpExecutorUtils.class);

    /**
     * @param queryAsString The query to execute.
     * @param backend The backend to use.
     * @param limit The number of scans to perform.
     * @return The random solutions mappings.
     */
    public static <ID,VALUE> Multiset<String> execute(String queryAsString, Backend<ID,VALUE,?> backend, Long limit) {
        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        RawOpExecutor<ID,VALUE> executor = new RawOpExecutor<ID,VALUE>()
                .setBackend(backend)
                .setLimit(limit);

        Iterator<BackendBindings<ID,VALUE>> iterator = executor.execute(query);

        Multiset<String> results = HashMultiset.create();
        while (iterator.hasNext()) {
            String binding = iterator.next().toString();
            results.add(binding);
            log.debug("{}", binding);
        }
        return results;
    }

    /**
     * @param queryAsString The query to execute.
     * @param executor The fully configured executor that will run the query.
     * @return The sample-based solutions mappings.
     */
    public static <ID,VALUE> Multiset<String> execute(String queryAsString, RawOpExecutor<ID,VALUE> executor) {
        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Iterator<BackendBindings<ID,VALUE>> iterator = executor.execute(query);

        Multiset<String> results = HashMultiset.create();
        while (iterator.hasNext()) {
            String binding = iterator.next().toString();
            results.add(binding);
            log.debug("{}", binding);
        }
        return results;
    }

    public static <ID,VALUE> Multiset<BackendBindings<?,?>> executeWithRaw(String queryAsString,
                                                                           Backend<ID,VALUE,?> backend,
                                                                           long limit) {
        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        RawOpExecutor<ID,VALUE> executor = new RawOpExecutor<ID,VALUE>()
                .setBackend(backend)
                .setLimit(limit);

        Iterator<BackendBindings<ID,VALUE>> iterator = executor.execute(query);

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (iterator.hasNext()) {
            BackendBindings<?,?> binding = iterator.next();
            results.add(binding);
            log.debug("{}", binding);
        }
        return results;
    }

    public static <ID,VALUE> Multiset<BackendBindings<?,?>> executeWithRaw(String queryAsString,
                                                                           Backend<ID,VALUE,?> backend,
                                                                           long limit, long timeout) {
        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        RawOpExecutor<ID,VALUE> executor = new RawOpExecutor<ID,VALUE>()
                .setBackend(backend)
                .setLimit(limit)
                .setTimeout(timeout);

        Iterator<BackendBindings<ID,VALUE>> iterator = executor.execute(query);

        Multiset<BackendBindings<?,?>> results = HashMultiset.create();
        while (iterator.hasNext()) {
            BackendBindings<?,?> binding = iterator.next();
            results.add(binding);
            log.debug("{}", binding);
        }
        return results;
    }

}