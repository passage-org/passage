package fr.gdd.sage.cli;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.generics.BackendBindings;
import fr.gdd.sage.sager.SagerOpExecutor;
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
        name = "sage",
        version = "0.0.4",
        description = "Preemptive SPARQL query processing. Yes, you can pause and resume your query execution!",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class SagerCLI {

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
        SagerCLI serverOptions = new SagerCLI();
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
            System.out.printf("%sPath to database:%s %s%n", PURPLE_BOLD, RESET, serverOptions.database);
            System.out.printf("%sSPARQL query:%s %s%n", PURPLE_BOLD, RESET, serverOptions.queryAsString);
        }

        for (int i = 0; i < serverOptions.numberOfExecutions; ++i) {
            SagerOpExecutor executor = new SagerOpExecutor();
            executor.setBackend(backend)
                    .setLimit(serverOptions.limit)
                    .setTimeout(serverOptions.timeout);

            if (serverOptions.forceOrder) {
                executor.forceOrder();
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
                System.out.printf("%sTo continue query execution, use the following query:%s%n%s%s%s%n", GREEN_UNDERLINED, RESET, ANSI_GREEN, executor.pauseAsString(), RESET);
            }
            System.gc(); // no guarantee but still
        }

        System.exit(CommandLine.ExitCode.OK);
    }

}
