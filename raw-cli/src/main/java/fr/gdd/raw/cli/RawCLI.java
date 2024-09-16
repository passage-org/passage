package fr.gdd.raw.cli;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.BlazegraphIterator;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.raw.accumulators.CountDistinctChaoLee;
import fr.gdd.raw.executor.RawOpExecutor;
import fr.gdd.raw.iterators.RandomAggregator;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Command Line Interface for running sampling-based SPARQL operators.
 */
@CommandLine.Command(
        name = "raw",
        version = "0.0.1",
        description = "RAndom-Walk-based SPARQL query processing.",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class RawCLI {

    public static final String YELLOW_BOLD = "\033[1;33m"; // YELLOW
    public static final String PURPLE_BOLD = "\033[1;35m"; // PURPLE
    public static final String RESET = "\033[0m";  // Text Reset

    @CommandLine.Option(
            order = 1,
            required = true,
            names = {"-d","--database"},
            paramLabel = "<path>",
            description = "The path to your blazegraph database.")
    String database;

    @CommandLine.Option(
            order = 2,
            names = {"-q", "--query"},
            paramLabel = "<SPARQL>",
            description = "The SPARQL query to execute.")
    String queryAsString;

    @CommandLine.Option(
            order = 2,
            names = {"-f", "--file"},
            paramLabel = "<path>",
            description = "The file containing the SPARQL query to execute.")
    String queryFile;

    @CommandLine.Option(
            order = 3,
            names = {"-t", "--timeout"},
            paramLabel = "<ms>",
            description = "Timeout before the query execution is stopped.")
    Long timeout = Long.MAX_VALUE;

    @CommandLine.Option(
            order = 3,
            names = {"-l", "--limit"},
            paramLabel = "<scans>",
            description = "Number of scans before the query execution is stopped.")
    Long limit = Long.MAX_VALUE;

    @CommandLine.Option(
            order = 4,
            names = {"-st", "--subtimeout"},
            paramLabel = "<ms>",
            description = "Timeout before the subquery execution is stopped (if exists).")
    Long subqueryTimeout = Long.MAX_VALUE;

    @CommandLine.Option(
            order = 4,
            names = {"-sl", "--sublimit"},
            paramLabel = "<scans>",
            description = "Number of scans before the subquery execution is stopped (if exists).")
    Long subqueryLimit = Long.MAX_VALUE;

    @CommandLine.Option(
            order = 11,
            names = {"-r", "--report"},
            description = "Provides a concise report on query execution.")
    Boolean report = false;

    @CommandLine.Option(
            order = 5,
            names = {"-cl", "--chao-lee"},
            description = "Use Chao-Lee as count-distinct estimator. Default is CRAWD.")
    Boolean chaolee = false;

    @CommandLine.Option(
            order = 5,
            names = "--force-order",
            description = "Force the order of triple patterns to the one provided by the query.")
    Boolean forceOrder = false;

    @CommandLine.Option(
            order = 6,
            names = "--seed",
            paramLabel = "1",
            description = "Set the seed of random number generators.")
    Integer seed = 1;

    @CommandLine.Option( // TODO by default, but then need to change XP parameters
            order = 7,
            names = "--no-seed",
            description = "Default is seeded, this disable it.")
    Boolean noSeed = false;

    @CommandLine.Option(
            order = 10,
            names = "--threads",
            paramLabel = "1",
            description = "Number of threads to process aggregate queries.")
    Integer maxThreads = 1;

    @CommandLine.Option(
            order = 11,
            names = {"-n", "--executions"},
            paramLabel = "1",
            description = "Number of times that it executes the query in sequence (for performance analysis).")
    Integer numberOfExecutions = 1;

    @CommandLine.Option(
            order = Integer.MAX_VALUE, // last
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Display this help message.")
    boolean usageHelpRequested;

    static AtomicInteger idProvider = new AtomicInteger(1);

    /* ****************************************************************** */

    public static void main(String[] args) {
        RawCLI serverOptions = new RawCLI();
        new CommandLine(serverOptions).parseArgs(args);

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile)) ||
                (Objects.isNull(serverOptions.timeout) && Objects.isNull(serverOptions.limit))) {
            CommandLine.usage(serverOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        // (big query limits subqueries)
        serverOptions.subqueryLimit = Math.min(serverOptions.subqueryLimit, serverOptions.limit);
        serverOptions.subqueryTimeout = Math.min(serverOptions.subqueryTimeout, serverOptions.timeout);

        if (Objects.nonNull(serverOptions.queryFile)) {
            Path queryPath = Path.of(serverOptions.queryFile);
            try {
                serverOptions.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.out.println("Error: could not read " + queryPath.toString() + ".");
                System.exit(CommandLine.ExitCode.SOFTWARE);
            }
        }

        if (Objects.isNull(serverOptions.numberOfExecutions)) {
            serverOptions.numberOfExecutions = 1;
        }

        if (Objects.isNull(serverOptions.timeout)) {
            serverOptions.timeout = Long.MAX_VALUE;
        }

        // TODO set the seed for any backend blazegraph or jena
        // /!\ this won't be enough when the thread's executor can have an input…
        // because the same thread must get the same id with the same input… which
        // is difficult to guarantee.
        if (!serverOptions.noSeed) {
            BlazegraphIterator.RNG = ThreadLocal.withInitial(() -> {
                long seed = serverOptions.seed + idProvider.getAndIncrement();
                return new Random(seed);
            });
        }

        // TODO database can be blazegraph or jena
        BlazegraphBackend backend = null;
        try {
            backend = new BlazegraphBackend(serverOptions.database);
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }

        if (serverOptions.report) {
            System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctChaoLee", "debug");
            System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctCRAWD", "debug");
            System.out.printf("%sPath to database:%s %s%n", PURPLE_BOLD, RESET, serverOptions.database);
            System.out.printf("%sSPARQL query:%s %s%n", PURPLE_BOLD, RESET, serverOptions.queryAsString);
        } else {
            System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctCRAWD", "error");
            System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctChaoLee", "error");
        }

        for (int i = 0; i < serverOptions.numberOfExecutions; ++i) {
            RandomAggregator.SUBQUERY_LIMIT = serverOptions.subqueryLimit; // ugly, but no better solutions rn
            RandomAggregator.SUBQUERY_TIMEOUT = serverOptions.subqueryTimeout;
            RawOpExecutor executor = new RawOpExecutor<>();
            executor.setBackend(backend)
                    .setLimit(serverOptions.limit)
                    .setTimeout(serverOptions.timeout)
                    .setMaxThreads(serverOptions.maxThreads);

            if (serverOptions.chaolee) {
                executor.setCountDistinct(CountDistinctChaoLee::new);
            }

            if (serverOptions.forceOrder) {
                executor.forceOrder();
            }

            // executor.setForceOrder();

            long start = System.currentTimeMillis();
            Iterator<BackendBindings> iterator = executor.execute(serverOptions.queryAsString);
            long nbResults = 0;
            while (iterator.hasNext()) { // TODO try catch
                System.out.println(iterator.next());
                nbResults += 1;
            }

            if (serverOptions.report) {
                long elapsed = System.currentTimeMillis() - start;
                System.out.printf("%sExecution time: %s %s ms%n", PURPLE_BOLD, RESET, elapsed);
                System.out.printf("%sNumber of Results: %s %s%n", PURPLE_BOLD, RESET, nbResults);
            }
            System.gc(); // no guarantee but still
        }

        System.exit(CommandLine.ExitCode.OK);
    }

}
