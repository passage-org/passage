package fr.gdd.passage.volcano;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.transforms.Quad2Pattern;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Provide simple classes to execute from the start to then end SPARQL queries
 * based on continuations.
 */
public class ExecutorUtils {

    public static Logger log = LoggerFactory.getLogger(ExecutorUtils.class);

    /**
     * @param queryAsString The SPARQL query to sample.
     * @param builder The builder that contains the whole environment to build the query engine.
     * @return The bag of mappings that constitutes the sampled results of the query.
     */
    public static <ID,VALUE> Multiset<BackendBindings<?,?>> executeOnce(String queryAsString, PassageExecutionContextBuilder<ID,VALUE> builder) {
        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        PassageExecutionContext<ID,VALUE> context = builder.build();
        log.debug("Initial query to sample {}", queryAsString);
        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Op paused = context.executor.execute(query, results::add); // log results in test if need be
        queryAsString = Objects.nonNull(paused) ? OpAsQuery.asQuery(new Quad2Pattern().visit(paused)).toString() : null;
        log.debug("Continuation for sampled query: {}", queryAsString);
        return results;
    }

    /**
     * @param queryAsString The SPARQL query to sample.
     * @param builder The builder that contains the whole environment to build the query engine.
     * @param consumer The consumer that will be applied to each result mapping.
     */
    public static <ID,VALUE> void executeOnce(String queryAsString, PassageExecutionContextBuilder<ID,VALUE> builder,  Consumer<BackendBindings<ID,VALUE>> consumer) {
        PassageExecutionContext<ID,VALUE> context = builder.build();
        log.debug("Initial query to sample {}", queryAsString);
        Op query = Algebra.compile(QueryFactory.create(queryAsString));
        Op paused = context.executor.execute(query, consumer); // log results in test if need be
        queryAsString = Objects.nonNull(paused) ? OpAsQuery.asQuery(new Quad2Pattern().visit(paused)).toString() : null;
        log.debug("Continuation for sampled query: {}", queryAsString);
    }

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param builder The builder that contains the whole environment to build the query engine.
     * @return The bag of mappings that constitutes the complete and correct results of the query.
     */
    public static <ID,VALUE> Multiset<BackendBindings<?,?>> execute(String queryAsString, PassageExecutionContextBuilder<ID,VALUE> builder) {
        Multiset<BackendBindings<?,?>> results = ConcurrentHashMultiset.create();
        int nbContinuations = -1;
        while (Objects.nonNull(queryAsString)) {
            nbContinuations++;
            PassageExecutionContext<?,?> context = builder.build();
            log.debug(queryAsString);
            Op query = Algebra.compile(QueryFactory.create(queryAsString));
            Op paused = context.executor.execute(query, (i) -> {
                log.debug("{}", i);
                results.add(i);
            });
            queryAsString = Objects.nonNull(paused) ? OpAsQuery.asQuery(new Quad2Pattern().visit(paused)).toString() : null;
        }
        if (nbContinuations > 0) log.debug("Number of continuations queries: {}.", nbContinuations);
        return results;
    }

    /**
     * @param queryAsString The SPARQL query to execute.
     * @param builder The builder that contains the whole environment to build the query engine.
     * @param consumer A function that is executed for each result of the query. This could serve,
     *                 for instance, as a counter of number of results (which is more efficient than
     *                 putting everything in a data structure to get its size).
     */
    public static <ID,VALUE> void execute(String queryAsString, PassageExecutionContextBuilder<ID,VALUE> builder, Consumer<BackendBindings<ID,VALUE>> consumer) {
        int nbContinuations = -1;
        while (Objects.nonNull(queryAsString)) {
            nbContinuations++;
            PassageExecutionContext<ID,VALUE> context = builder.build();
            Op query = Algebra.compile(QueryFactory.create(queryAsString));
            log.debug(queryAsString);
            Op paused = context.executor.execute(query, (i) -> {
                log.debug("{}", i);
                consumer.accept(i);
            });
            queryAsString = Objects.nonNull(paused) ? OpAsQuery.asQuery(new Quad2Pattern().visit(paused)).toString() : null;
        }
        if (nbContinuations > 0) log.debug("Number of continuations queries: {}.", nbContinuations);
    }
}