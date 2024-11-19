package fr.gdd.raw.cli;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.io.ExtensibleRowSetWriterJSON;
import fr.gdd.passage.commons.io.ModuleOutputRegistry;
import fr.gdd.raw.cli.server.RawOpExecutorFactory;
import fr.gdd.raw.cli.server.RawOperation;
import fr.gdd.raw.cli.server.RawQueryEngine;
import fr.gdd.raw.cli.server.RawOutputWriterJSON;
import fr.gdd.raw.executor.RawConstants;
import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.mgt.ActionServerStatus;
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
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.Objects;

import static fr.gdd.raw.cli.RawCLI.PURPLE_BOLD;
import static fr.gdd.raw.cli.RawCLI.RESET;

@CommandLine.Command(
        name = "raw-server",
        version = "0.0.3",
        description = "A server for preemptive SPARQL query processing!",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class RawServerCLI {
    @CommandLine.Option(
            order = 1,
            required = true,
            names = {"-d","--database"},
            paramLabel = "<path>",
            description = "The path to your blazegraph database.")
    String database;

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
            order = 3,
            names = {"-al", "--attempt-limit"},
            paramLabel = "<randomwalkattempt>",
            description = "Number of random walk attempts before the query execution is stopped.")
    Long attemptLimit = Long.MAX_VALUE;

//    @CommandLine.Option(
//            order = 5,
//            names = "--force-order",
//            description = "Force the order of triple patterns to the one provided by the query.")
//    Boolean forceOrder = false; // TODO

    @CommandLine.Option(
            order = 6,
            names = "--ui", description = "The path to your UI folder.")
    public String ui;

    @CommandLine.Option(
            order = 6,
            names = {"-p", "--port"},
            paramLabel = "<3330>",
            description = "The port of the server.")
    public Integer port = 3330;

    @CommandLine.Option(
            order = Integer.MAX_VALUE, // last
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Display this help message.")
    boolean usageHelpRequested;

    /* ***********************************************************************/

    public static void main(String[] args) {
        RawServerCLI serverOptions = new RawServerCLI();

        try {
            new CommandLine(serverOptions).parseArgs(args);
        } catch (Exception e) {
            CommandLine.usage(serverOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        if (serverOptions.usageHelpRequested || Objects.isNull(serverOptions.database)) {
            CommandLine.usage(serverOptions, System.out);
            System.exit(CommandLine.ExitCode.USAGE);
        }

        // TODO database can be blazegraph, jena, or hdt
        // TODO think about creating a new database at desired location if
        //  nothing a path to nothing is provided.
        BlazegraphBackend backend = null;
        try {
            backend = new BlazegraphBackend(serverOptions.database);
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }

        String name = Path.of(serverOptions.database).getFileName().toString();

        System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctChaoLee", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctCRAWD", "debug");
        System.out.printf("%sPath to database:%s %s%n", PURPLE_BOLD, RESET, serverOptions.database);
        System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctCRAWD", "error");
        System.setProperty("org.slf4j.simpleLogger.log.fr.gdd.sage.rawer.accumulators.CountDistinctChaoLee", "error");

        FusekiServer server = buildServer(name, backend,
                serverOptions.timeout,
                serverOptions.port, serverOptions.ui, serverOptions.limit, serverOptions.attemptLimit);

        server.start();
    }


    /**
     * Build a Raw fuseki server.
     * @param backend A backend that supports preemption.
     * @param ui The path to the ui.
     * @return A fuseki server not yet running.
     */
    static FusekiServer buildServer(String name, Backend<?, ?, Long> backend,
                                    Long timeout,
                                    Integer port,
                                    String ui,
                                    Long limit,
                                    Long attemptLimit) {
        // ARQ.setExecutionLogging(Explain.InfoLevel.ALL);
        // wraps our database inside a standard but empty dataset.
        ARQ.enableOptimizer(false); // just in case

        Dataset dataset = DatasetFactory.create(); // TODO double check if it's alright
        dataset.getContext().set(BackendConstants.BACKEND, backend);
        dataset.getContext().set(RawConstants.LIMIT, limit);
        dataset.getContext().setIfUndef(RawConstants.ATTEMPT_LIMIT, attemptLimit);
//        dataset.getContext().set(PassageConstants.TIMEOUT, timeout);
        QC.setFactory(dataset.getContext(), new RawOpExecutorFactory());
        QueryEngineRegistry.addFactory(RawQueryEngine.factory);

        // set globally but the dedicated writter of sage only comes into
        // play when some variables exist in the execution context.
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, ExtensibleRowSetWriterJSON.factory);
        ModuleOutputRegistry.register(ResultSetLang.RS_JSON, new RawOutputWriterJSON());

        // FusekiModules.add(new SageModule());

        FusekiServer.Builder serverBuilder = FusekiServer.create()
                // .parseConfigFile("configurations/sage.ttl") // TODO let it be a configuration file
                .port(port)
                .enablePing(true)
                .enableCompact(true)
                .enableCors(true, "passage-cli/src/main/resources/cors.config")
                .enableStats(true)
                .enableTasks(true)
                .enableMetrics(true)
                .numServerThreads(1, 10)
                // .loopback(false)
                .serverAuthPolicy(Auth.ANY_ANON) // Anyone can access the server
                .addProcessor("/$/server", new ActionServerStatus())
                //.addProcessor("/$/datasets/*", new ActionDatasets())
                .registerOperation(RawOperation.Raw, new SPARQL_QueryDataset())
                .addDataset(name, dataset.asDatasetGraph()) // register the dataset
                .addEndpoint(name, "raw", RawOperation.Raw)
                // .auth(AuthScheme.BASIC)
                // .addEndpoint(name, name+"/meow", SageOperation.Sage, Auth.ANY_ANON)
                ;

        if (Objects.nonNull(ui)) { // add UI if need be
            serverBuilder.staticFileBase(ui);
        }

        return serverBuilder.build();

    }
}
