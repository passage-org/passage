package fr.gdd.passage.cli;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendBindings;
import fr.gdd.passage.volcano.PassageOpExecutor;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;

/**
 * Command Line Interface for running sampling-based SPARQL operators.
 */
@CommandLine.Command(
        name = "passage",
        version = "0.0.1",
        description = "Preemptive SPARQL query processing. Yes, you can pause and resume your query execution!",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class PassageCLI {

    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String GREEN_UNDERLINED = "\033[4;32m";
    public static final String YELLOW_BOLD = "\033[1;33m";
    public static final String PURPLE_BOLD = "\033[1;35m";
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
            names = {"--loop"},
            description = "Continue executing the query until completion.")
    Boolean loop = false;


    @CommandLine.Option(
            order = 11,
            names = {"-r", "--report"},
            description = "Provides a concise report on query execution.")
    Boolean report = false;

    @CommandLine.Option(
            order = 5,
            names = "--force-order",
            description = "Force the order of triple patterns to the one provided by the query.")
    Boolean forceOrder = false;

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

    /* ****************************************************************** */

    public static void main(String[] args) {
        PassageCLI serverOptions = new PassageCLI();
        new CommandLine(serverOptions).parseArgs(args);

        if (serverOptions.usageHelpRequested ||
                Objects.isNull(serverOptions.database) ||
                (Objects.isNull(serverOptions.queryAsString) && Objects.isNull(serverOptions.queryFile)) ||
                (Objects.isNull(serverOptions.timeout) && Objects.isNull(serverOptions.limit))) {
            CommandLine.usage(serverOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (Objects.nonNull(serverOptions.queryFile)) {
            Path queryPath = Path.of(serverOptions.queryFile);
            try {
                serverOptions.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.out.println("Error: could not read " + queryPath + ".");
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
        BlazegraphBackend backend = null;
        try {
            backend = new BlazegraphBackend(serverOptions.database);
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }

        if (serverOptions.report) {
            System.err.printf("%sPath to database:%s %s%n", PURPLE_BOLD, RESET, serverOptions.database);
            System.err.printf("%sSPARQL query:%s %s%n", PURPLE_BOLD, RESET, serverOptions.queryAsString);
        }

        for (int i = 0; i < serverOptions.numberOfExecutions; ++i) {
            String queryToRun = serverOptions.queryAsString;
            long totalElapsed = 0L;
            long totalNbResults = 0L;
            long totalPreempt = -1L; // start -1 because the first execution is not considered
            do {
                PassageOpExecutor executor = new PassageOpExecutor();
                executor.setBackend(backend)
                        .setLimit(serverOptions.limit)
                        .setTimeout(serverOptions.timeout);

                if (serverOptions.forceOrder) {
                    executor.forceOrder();
                }

                long start = System.currentTimeMillis();
                Iterator<BackendBindings> iterator = executor.execute(queryToRun);
                long nbResults = 0;
                while (iterator.hasNext()) { // TODO try catch
                    System.out.println(iterator.next());
                    nbResults += 1;
                }
                long elapsed = System.currentTimeMillis() - start;

                totalElapsed += elapsed;
                totalNbResults += nbResults;
                totalPreempt += 1;

                queryToRun = executor.pauseAsString();

                if (serverOptions.report) {
                    System.err.printf("%sNumber of pause/resume: %s %s%n", PURPLE_BOLD, RESET, totalPreempt);
                    System.err.printf("%sExecution time: %s %s ms%n", PURPLE_BOLD, RESET, elapsed);
                    System.err.printf("%sNumber of results: %s %s%n", PURPLE_BOLD, RESET, nbResults);
                    if (Objects.nonNull(queryToRun)) {
                        System.err.printf("%sTo continue query execution, use the following query:%s%n%s%s%s%n", GREEN_UNDERLINED, RESET, ANSI_GREEN, queryToRun, RESET);
                    } else {
                        System.err.printf("%sThe query execution is complete.%s%n", GREEN_UNDERLINED, RESET);
                    }
                }

            } while (Objects.nonNull(queryToRun) && serverOptions.loop);

            if (serverOptions.report && serverOptions.loop) {
                System.err.println();
                System.err.printf("%sTOTAL number of pause/resume: %s %s%n", PURPLE_BOLD, RESET, totalPreempt);
                System.err.printf("%sTOTAL execution time: %s %s ms%n", PURPLE_BOLD, RESET, totalElapsed);
                System.err.printf("%sTOTAL number of results: %s %s%n", PURPLE_BOLD, RESET, totalNbResults);
            }
            System.gc(); // no guarantee but still
        }

        System.exit(CommandLine.ExitCode.OK);
    }

}
