package fr.gdd.sage.rawer.cli;

// TODO TODO TODO

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.rawer.RawerOpExecutor;
import fr.gdd.sage.rawer.accumulators.CountDistinctChaoLee;
import fr.gdd.sage.rawer.iterators.RandomAggregator;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;

/**
 * Command Line Interface for running sampling-based SPARQL operators.
 */
public class RawerCLI {

    public static final String YELLOW_BOLD = "\033[1;33m"; // YELLOW
    public static final String PURPLE_BOLD = "\033[1;35m"; // PURPLE
    public static final String RESET = "\033[0m";  // Text Reset

    @CommandLine.Option(names = {"-d","--database"},
            description = "The path to your blazegraph database.")
    String database;

    @CommandLine.Option(names = {"-n", "--executions"},
            description = "Number of times that it executes the query in sequence (for performance analysis).")
    Integer numberOfExecutions = 1;

    @CommandLine.Option(names = {"-q", "--query"},
            description = "The SPARQL query to execute.")
    String queryAsString;

    @CommandLine.Option(names = {"-f", "--file"},
            description = "The file containing the SPARQL query to execute.")
    String queryFile;

    @CommandLine.Option(names = {"-t", "--timeout"},
            description = "Timeout before the query execution is stopped.")
    Long timeout = Long.MAX_VALUE;

    @CommandLine.Option(names = {"-l", "--limit"},
            description = "Number of scans before the query execution is stopped.")
    Long limit = Long.MAX_VALUE;

    @CommandLine.Option(names = {"-st", "--subtimeout"},
            description = "Timeout before the subquery execution is stopped (if exists).")
    Long subquerytimeout = Long.MAX_VALUE;

    @CommandLine.Option(names = {"-sl", "--sublimit"},
            description = "Number of scans before the subquery execution is stopped (if exists).")
    Long subqueryLimit = Long.MAX_VALUE;

    @CommandLine.Option(names = {"-r", "--report"},
            description = "Provides a concise report on query execution.")
    Boolean report = false;

    @CommandLine.Option(names = {"-cl", "--chao-lee"},
            description = "Use Chao-Lee as count-distinct estimator.")
    Boolean chaolee = false;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Display this help message.")
    boolean usageHelpRequested;

    /* ****************************************************************** */

    public static void main(String[] args) {
        RawerCLI serverOptions = new RawerCLI();
        new CommandLine(serverOptions).parseArgs(args);

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile)) ||
                (Objects.isNull(serverOptions.timeout) && Objects.isNull(serverOptions.limit))) {
            CommandLine.usage(new RawerCLI(), System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        // (big query limits subqueries)
        serverOptions.subqueryLimit = Math.min(serverOptions.subqueryLimit, serverOptions.limit);
        serverOptions.subquerytimeout = Math.min(serverOptions.subquerytimeout, serverOptions.timeout);

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

        // TODO database can be blazegraph or jena
        BlazegraphBackend backend = new BlazegraphBackend(serverOptions.database);

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
            RandomAggregator.SUBQUERY_TIMEOUT = serverOptions.subquerytimeout;
            RawerOpExecutor executor = new RawerOpExecutor<>();
            executor.setBackend(backend)
                    .setLimit(serverOptions.limit)
                    .setTimeout(serverOptions.timeout);

            if (serverOptions.chaolee) {
                executor.setCountDistinct(CountDistinctChaoLee::new);
            }

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
