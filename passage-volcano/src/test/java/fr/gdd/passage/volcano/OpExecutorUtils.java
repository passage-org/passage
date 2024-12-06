package fr.gdd.passage.volcano;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.pull.PassagePullExecutor;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.transforms.Quad2Pattern;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

public class OpExecutorUtils {

    private static final Logger log = LoggerFactory.getLogger(OpExecutorUtils.class);

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, Backend<?,?> backend) {
        return executeWithPassage(queryAsString, new PassageExecutionContextBuilder().setBackend(backend).build());
    }

    public static Multiset<BackendBindings<?,?>> executeWithPassage(String queryAsString, PassageExecutionContext<?,?> ec) {
        PassagePullExecutor<?,?> executor = new PassagePullExecutor<>(ec);

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

    public static long countWithPassage(String queryAsString, Backend<?,?> backend) {
        return countWithPassage(queryAsString, new PassageExecutionContextBuilder().setBackend(backend).build());
    }

    public static long countWithPassage(String queryAsString, PassageExecutionContext<?,?> ec) {
        PassagePullExecutor<?,?> executor = new PassagePullExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Iterator<? extends BackendBindings<?, ?>> iterator = executor.execute(query);

        long count = 0;
        while (iterator.hasNext()) {
            BackendBindings<?,?> ignored = iterator.next();
            count += 1;
        }
        return count;
    }

    /* ************************************************************************** */

    public static Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, Backend<?,?> backend) {
        return executeWithPush(queryAsString, new PassageExecutionContextBuilder().
                setBackend(backend)
                .build());
    }

    public static Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, Backend<?,?> backend,
                                                                 int maxParallel) {
        return executeWithPush(queryAsString, new PassageExecutionContextBuilder()
                .setBackend(backend)
                .setMaxParallel(maxParallel)
                .build());
    }

    public static Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, Backend<?,?> backend,
                                                                 int maxParallel,
                                                                 Consumer<BackendBindings<?,?>> consumer) {
        return executeWithPush(queryAsString, new PassageExecutionContextBuilder()
                .setBackend(backend)
                .setMaxParallel(maxParallel)
                .build(), consumer);
    }

    public static Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, PassageExecutionContext<?,?> ec) {
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        executor.execute(query, result -> {
            log.debug("{}", result);
            results.add(result);
        });

        return results;
    }

    public static Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, PassageExecutionContext<?,?> ec, Consumer<BackendBindings<?,?>> consumer) {
        PassagePushExecutor<?,?> executor = new PassagePushExecutor<>(ec);

        Op query = Algebra.compile(QueryFactory.create(queryAsString));

        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        executor.execute(query,consumer::accept);
        return results;
    }

    public static <ID,VALUE> Multiset<BackendBindings<?,?>> executeWithPush(String queryAsString, PassageExecutionContextBuilder<ID,VALUE> builder) {
        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        while (Objects.nonNull(queryAsString)) {
            PassagePushExecutor<ID, VALUE> executor = new PassagePushExecutor<>(builder.build());
            Op query = Algebra.compile(QueryFactory.create(queryAsString));
            log.debug(queryAsString);
            Op paused = executor.execute(query, (i) -> {
                log.debug("{}", i);
                results.add(i);
            });
            queryAsString = Objects.nonNull(paused) ? OpAsQuery.asQuery(new Quad2Pattern().visit(paused)).toString() : null;
        }
        return results;
    }
}