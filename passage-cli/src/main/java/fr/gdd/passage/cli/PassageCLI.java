package fr.gdd.passage.cli;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.hdt.HDTBackend;
import fr.gdd.passage.volcano.PassageExecutionContext;
import fr.gdd.passage.volcano.PassageExecutionContextBuilder;
import fr.gdd.passage.volcano.push.PassagePushExecutor;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * Command Line Interface for running sampling-based SPARQL operators.
 */
@CommandLine.Command(
        name = "passage",
        version = "0.1.0",
        description = "SPARQL continuation query processing. Looping until done!",
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
            description = "The path to your blazegraph|hdt database.")
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
            order = 5,
            names = "--force-order",
            description = "Force the order of triple patterns to the one provided by the query.")
    Boolean forceOrder = false;

    @CommandLine.Option(
            order = 5,
            paramLabel = "1",
            names = "--threads",
            description = "Number of threads dedicated to execute the query.")
    Integer threads = 1;

    @CommandLine.Option(
            order = 11,
            names = {"-r", "--report"},
            description = "Provides a concise report on query execution.")
    Boolean report = false;

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

    private final static Logger log = LoggerFactory.getLogger(PassageCLI.class);

    /* ****************************************************************** */

    public static void main(String[] args) {
        PassageCLI passageOptions = new PassageCLI();

        try {
            new CommandLine(passageOptions).parseArgs(args);
        } catch (Exception e) {
            log.error("Failed to parse command line arguments");
            CommandLine.usage(passageOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (passageOptions.usageHelpRequested ||
                Objects.isNull(passageOptions.database) ||
                (Objects.isNull(passageOptions.queryAsString) && Objects.isNull(passageOptions.queryFile)) ||
                (Objects.isNull(passageOptions.timeout) && Objects.isNull(passageOptions.limit))) {
            CommandLine.usage(passageOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (Objects.nonNull(passageOptions.queryFile)) {
            Path queryPath = Path.of(passageOptions.queryFile);
            try {
                passageOptions.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                log.error("Error: could not read {}.", queryPath);
                System.exit(CommandLine.ExitCode.USAGE);
            }
        }

        if (Objects.isNull(passageOptions.numberOfExecutions)) { passageOptions.numberOfExecutions = 1; }
        if (Objects.isNull(passageOptions.timeout)) { passageOptions.timeout = Long.MAX_VALUE; }

        Backend<?,?> backend = null;
        try {
            backend = getBackend(passageOptions.database);
        } catch (SailException | RepositoryException | IOException e) {
            log.error("Error: could not get backend: {}", e.getMessage());
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (passageOptions.report) {
            log.debug("{}Path to database:{} {}", PURPLE_BOLD, RESET, passageOptions.database);
            log.debug("{}SPARQL query:{} {}", PURPLE_BOLD, RESET, passageOptions.queryAsString);
        }

        for (int i = 0; i < passageOptions.numberOfExecutions; ++i) {
            String queryToRun = passageOptions.queryAsString;
            long totalElapsed = 0L;
            long totalNbResults = 0L;
            long totalPreempt = -1L; // start -1 because the first execution is not considered
            do {
                PassageExecutionContext<?,?> context = new PassageExecutionContextBuilder<>()
                        .setName("PUSH")
                        .setBackend(backend)
                        .setMaxScans(passageOptions.limit)
                        .setTimeout(passageOptions.timeout)
                        .forceOrder(passageOptions.forceOrder)
                        .setMaxParallel(passageOptions.threads)
                        .setExecutorFactory((ec) -> new PassagePushExecutor<>((PassageExecutionContext<?,?>) ec))
                        .build();

                final LongAdder nbResults = new LongAdder();
                long start = System.currentTimeMillis();
                Op paused = context.executor.execute(queryToRun, (result) -> {
                    log.info("{}", result);
                    nbResults.increment();
                });
                long elapsed = System.currentTimeMillis() - start;

                totalElapsed += elapsed;
                totalNbResults += nbResults.longValue();
                totalPreempt += 1;

                queryToRun = Objects.nonNull(paused) ? QueryFactory.create(OpAsQuery.asQuery(paused)).toString() : null;

                if (passageOptions.report) {
                    log.debug("{}Number of pause/resume: {} {}", PURPLE_BOLD, RESET, totalPreempt);
                    log.debug("{}Execution time: {} {} ms", PURPLE_BOLD, RESET, elapsed);
                    log.debug("{}Number of results: {} {}", PURPLE_BOLD, RESET, nbResults);
                    log.debug("{}TOTAL Number of results: {} {}", PURPLE_BOLD, RESET, totalNbResults);
                    if (Objects.nonNull(queryToRun)) {
                        log.debug("\n{}To continue query execution, use the following query:{}\n{}{}{}", GREEN_UNDERLINED, RESET, ANSI_GREEN, queryToRun, RESET);
                    } else {
                        log.debug("\n{}The query execution is complete.{}", GREEN_UNDERLINED, RESET);
                    }
                }

            } while (Objects.nonNull(queryToRun) && passageOptions.loop);

            if (passageOptions.report && passageOptions.loop) {
                log.debug("");
                log.debug("{}TOTAL number of pause/resume: {} {}", PURPLE_BOLD, RESET, totalPreempt);
                log.debug("{}TOTAL execution time: {} {} ms", PURPLE_BOLD, RESET, totalElapsed);
                log.debug("{}TOTAL number of results: {} {}", PURPLE_BOLD, RESET, totalNbResults);
            }
            System.gc(); // no guarantee but still
        }

        try {
            backend.close();
        } catch (Exception e) {
            log.error("Error: could not close backend: {}", e.getMessage());
            System.exit(CommandLine.ExitCode.SOFTWARE);
        }

        System.exit(CommandLine.ExitCode.OK);
    }

    /* ************************************************************************************ */

    /**
     * Gather all kinds of backend handled in this repository.
     * @param toDatabase The path to the database as a string.
     * @return The backend as a generic interface.
     */
    public static Backend<?,?> getBackend (String toDatabase) throws IOException, SailException, RepositoryException {
        Path pathToDatabase = Path.of(toDatabase);
        if (!pathToDatabase.toFile().exists()) {
            throw new IOException("The path to the database does not seem right.");
        }

        String extension = FilenameUtils.getExtension(pathToDatabase.toString());
        return switch (extension) {
            case "jnl" -> new BlazegraphBackend(toDatabase);
            case "hdt" -> new HDTBackend(toDatabase);
            default -> throw new IOException("The database is of unknown type.");
        };
    }

}
