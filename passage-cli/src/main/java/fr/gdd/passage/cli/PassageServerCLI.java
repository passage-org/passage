package fr.gdd.passage.cli;

import fr.gdd.passage.cli.server.PassageOpExecutorFactory;
import fr.gdd.passage.cli.server.PassageOperation;
import fr.gdd.passage.cli.server.PassageOutputWriterJSON;
import fr.gdd.passage.cli.server.PassageQueryEngine;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.io.ExtensibleRowSetWriterJSON;
import fr.gdd.passage.commons.io.ModuleOutputRegistry;
import fr.gdd.passage.volcano.PassageConstants;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.QC;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Objects;

/**
 * Command Line Interface of the Fuseki server running Passage. /!\ it does not
 * intend to replace Apache Jena. It only runs 1 Passage server at a time, defined
 * in the arguments.
 */
@CommandLine.Command(
        name = "passage-server",
        version = "0.1.0",
        description = "A server for complete SPARQL query processing! using continuation queries.",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class PassageServerCLI {

    @CommandLine.Option(
            order = 1,
            required = true,
            names = {"-d","--database"},
            paramLabel = "<path/to/database>",
            description = "The path to your database.")
    String database;

    @CommandLine.Option(
            order = 3,
            names = {"-t", "--timeout"},
            paramLabel = "<ms>",
            description = "Timeout before the query execution is stopped.")
    Long timeout = Long.MAX_VALUE;

    @CommandLine.Option(
            order = 5,
            names = "--force-order",
            description = "Force the order of triple/quad patterns to the one provided by the query.")
    Boolean forceOrder = false;

    @CommandLine.Option(
            order = 5,
            paramLabel = "1",
            names = "--threads",
            description = "Number of threads dedicated to execute the query.")
    Integer threads = 1;

    @CommandLine.Option(
            order = 6,
            paramLabel = "<path/to/folder/containing/index/dot/html/>",
            names = "--ui", description = "The path to your UI folder.")
    public String ui;

    @CommandLine.Option(
            order = 6,
            paramLabel = "<path/to/cors/file>",
            names = "--cors", description = "The path to a CORS configuration file.")
    public String cors;

    @CommandLine.Option(
            order = 6,
            paramLabel = "<path/to/fuseki/config.ttl>",
            names = "--config", description = "The path to an Apache Fuseki configuration file.")
    public String config;

    @CommandLine.Option(
            order = 6,
            names = {"-p", "--port"},
            paramLabel = "3330",
            description = "The port of the server.")
    public Integer port = 3330;

    @CommandLine.Option(
            order = Integer.MAX_VALUE, // last
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Display this help message.")
    boolean usageHelpRequested;

    private final static Logger log = LoggerFactory.getLogger(PassageServerCLI.class);

    /* ***********************************************************************/

    public static void main(String[] args) {
        PassageServerCLI serverOptions = new PassageServerCLI();

        try {
            new CommandLine(serverOptions).parseArgs(args);
        } catch (Exception e) {
            log.error("Failed to parse command line arguments.");
            CommandLine.usage(serverOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (serverOptions.usageHelpRequested || Objects.isNull(serverOptions.database)) {
            CommandLine.usage(serverOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        Backend<?,?> backend = null;
        try {
            backend = PassageCLI.getBackend(serverOptions.database);
        } catch (SailException | RepositoryException | IOException e) {
            log.error("Error: could not get backend: {}", e.getMessage());
            System.exit(CommandLine.ExitCode.USAGE);
        }

        String name = FilenameUtils.getBaseName(serverOptions.database);

        FusekiServer server = buildServer(name, backend, serverOptions);

        server.start();
    }


    /**
     * @return An Apache Fuseki server based on parameters, not yet running.
     */
    static FusekiServer buildServer(String name, Backend<?, ?> backend, PassageServerCLI options) {
        // ARQ.setExecutionLogging(Explain.InfoLevel.ALL);
        // wraps our database inside a standard but empty dataset.
        ARQ.enableOptimizer(false); // just in case

        Dataset dataset = DatasetFactory.create(); // TODO double check if it's alright
        dataset.getContext().set(BackendConstants.BACKEND, backend);
        dataset.getContext().set(PassageConstants.TIMEOUT, options.timeout);
        QC.setFactory(dataset.getContext(), new PassageOpExecutorFactory());
        QueryEngineRegistry.addFactory(PassageQueryEngine.factory);

        // set globally but the dedicated writer only comes into
        // play when some variables exist in the execution context.
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, ExtensibleRowSetWriterJSON.factory);
        ModuleOutputRegistry.register(ResultSetLang.RS_JSON, new PassageOutputWriterJSON());

        // if need be, processors must be added using the Apache Fuseki configuration file
        FusekiServer.Builder serverBuilder = FusekiServer.create()
                .port(options.port)
                .serverAuthPolicy(Auth.ANY_ANON); // Anyone can access the server

        if (Objects.nonNull(backend)) {
            serverBuilder.registerOperation(PassageOperation.Passage, new SPARQL_QueryDataset())
                    .addDataset(name, dataset.asDatasetGraph())
                    .addEndpoint(name, "passage", PassageOperation.Passage);
        }

        if (Objects.nonNull(options.config)) {
            serverBuilder.parseConfigFile(options.config);
        }

        if (Objects.nonNull(options.cors)) {
            serverBuilder.enableCors(true, null);
        }

        if (Objects.nonNull(options.ui)) { // add UI if need be
            serverBuilder.staticFileBase(options.ui);
        }

        return serverBuilder.build();

    }
}
