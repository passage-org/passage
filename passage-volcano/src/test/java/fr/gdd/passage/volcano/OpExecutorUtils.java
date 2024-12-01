package fr.gdd.passage.volcano;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.spliterators.PassagePushExecutor;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class OpExecutorUtils {

    private static final Logger log = LoggerFactory.getLogger(OpExecutorUtils.class);

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, Backend<?,?,?> backend) {
        return executeWithPassage(queryAsString, new PassageExecutionContextBuilder().setBackend(backend).build());
    }

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, PassageExecutionContext<?,?> ec) {
        PassageOpExecutor<?,?> executor = new PassageOpExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Iterator<? extends BackendBindings<?, ?>> iterator = executor.execute(query);

        Multiset<BackendBindings<?,?>> bindings = HashMultiset.create();
        while (iterator.hasNext()) {
            BackendBindings<?,?> binding = iterator.next();
            bindings.add(binding);
            log.debug("{}: {}", bindings.size(), binding);

        }
        return bindings;
    }

    /* ************************************************************************** */

    public static Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, Backend<?,?,?> backend) {
        return executeWithPush(queryAsString, new PassageExecutionContextBuilder().setBackend(backend).build());
    }

    public static Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, PassageExecutionContext<?,?> ec) {
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        try (ForkJoinPool customPool = new ForkJoinPool(10)) {
            customPool.submit( () -> executor.execute(query).forEach(result -> {
                        log.debug("{}", result);
                        results.add(result);
            })).join();
        }
        return results;
    }

    /* ********************************************************************* */

    public static long countWithPush(String queryAsString, Backend<?,?,?> backend) {
        return countWithPush(queryAsString, new PassageExecutionContextBuilder().setBackend(backend).build());
    }

    public static long countWithPush(String queryAsString, PassageExecutionContext<?,?> ec) {
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        AtomicLong nbResults = new AtomicLong();
        try (ForkJoinPool customPool = new ForkJoinPool(1)) {
            customPool.submit( () -> executor.execute(query).forEach(ignored -> nbResults.getAndIncrement()) )
                    .join();
        }
        return nbResults.get();
    }


}