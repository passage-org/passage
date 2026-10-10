package fr.gdd.passage.volcano.benchmarks;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.qlever.QLeverBackend;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import fr.gdd.passage.volcano.transforms.Quad2Pattern;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * One query, one process, every solution: the wall time a first query takes,
 * which the repeated runs of a benchmark deliberately do not measure.
 *
 * <p>Run it once per backend, each in its own JVM, so that neither inherits a
 * compiled pipeline from the other:
 *
 * <pre>
 * java -cp … ColdStart qlever     /path/to/index/basename
 * java -cp … ColdStart blazegraph /path/to/journal.jnl
 * </pre>
 *
 * <p>It reports the time to open the store, the time to run the query to
 * completion, and the time since the JVM itself started, which is what someone
 * waiting for an answer actually waits.
 */
public class ColdStart {

    static final String QUERY = "SELECT * WHERE { ?s a ?c . ?s ?p ?o }";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: ColdStart <qlever|blazegraph> <path>");
            System.exit(2);
        }
        Instant jvmStart = ProcessHandle.current().info().startInstant().orElse(Instant.now());

        Instant beforeOpen = Instant.now();
        Backend<?, ?> backend = args[0].equalsIgnoreCase("qlever")
                ? new QLeverBackend(args[1])
                : new BlazegraphBackend(args[1]);
        Duration open = Duration.between(beforeOpen, Instant.now());

        String sparql = args.length >= 3 ? args[2] : QUERY;
        Instant beforeQuery = Instant.now();
        long solutions = execute(backend, sparql);
        Duration query = Duration.between(beforeQuery, Instant.now());

        System.out.printf("%s  open %,d ms  query %,d ms  since JVM start %,d ms  "
                        + "%,d solutions  (%,d /s)%n",
                args[0], open.toMillis(), query.toMillis(),
                Duration.between(jvmStart, Instant.now()).toMillis(), solutions,
                query.toMillis() == 0 ? 0 : solutions * 1000 / query.toMillis());
        backend.close();
        // The JVM dies with a SIGSEGV in jemalloc's cleanup when the QLever
        // bridge has been loaded, after the work is done; see the module's
        // README. Leave before that, so the exit status means something.
        Runtime.getRuntime().halt(0);
    }

    static <ID, VALUE> long execute(Backend<ID, VALUE> backend, String query) {
        LongAdder counter = new LongAdder();
        String current = query;
        while (Objects.nonNull(current)) {
            PassageExecutionContext<ID, VALUE> context =
                    new PassageExecutionContextBuilder<ID, VALUE>()
                            .setName("PUSH")
                            .setMaxParallel(1)
                            .setBackend(backend)
                            .setExecutorFactory(ec ->
                                    new PassagePushExecutor<>((PassageExecutionContext<?, ?>) ec))
                            .build();
            Op paused = context.executor.execute(
                    Algebra.compile(QueryFactory.create(current)),
                    solution -> counter.increment());
            current = Objects.nonNull(paused)
                    ? OpAsQuery.asQuery(new Quad2Pattern().visit(paused)).toString()
                    : null;
        }
        return counter.sum();
    }
}
