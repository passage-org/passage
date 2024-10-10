package fr.gdd.passage.cli;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.cli.server.PassageOperation;
import fr.gdd.passage.cli.writers.ExtensibleRowSetWriterJSON;
import fr.gdd.passage.cli.writers.ModuleOutputRegistry;
import fr.gdd.passage.cli.writers.OutputWriterJSONPassage;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.volcano.PassageConstants;
import fr.gdd.passage.cli.server.PassageOpExecutorFactory;
import fr.gdd.passage.cli.server.PassageQueryEngine;
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

/**
 * Command Line Interface of the Fuseki server running Passage.
 */
@CommandLine.Command(
        name = "passage-server",
        version = "0.0.1",
        description = "A server for preemptive SPARQL query processing!",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class PassageServerCLI {

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
        PassageServerCLI serverOptions = new PassageServerCLI();

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
        FusekiServer server = buildServer(name, backend,
                serverOptions.timeout,
                serverOptions.port, serverOptions.ui);

        server.start();
    }


    /**
     * Build a Passage fuseki server.
     * @param backend A backend that supports preemption.
     * @param ui The path to the ui.
     * @return A fuseki server not yet running.
     */
    static FusekiServer buildServer(String name, Backend<?, ?, Long> backend,
                                    Long timeout,
                                    Integer port,
                                    String ui) {
        // ARQ.setExecutionLogging(Explain.InfoLevel.ALL);
        // wraps our database inside a standard but empty dataset.
        ARQ.enableOptimizer(false); // just in case

        Dataset dataset = DatasetFactory.create(); // TODO double check if it's alright
        dataset.getContext().set(PassageConstants.BACKEND, backend);
        dataset.getContext().set(PassageConstants.TIMEOUT, timeout);
        QC.setFactory(dataset.getContext(), new PassageOpExecutorFactory());
        QueryEngineRegistry.addFactory(PassageQueryEngine.factory);

        // set globally but the dedicated writter of sage only comes into
        // play when some variables exist in the execution context.
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, ExtensibleRowSetWriterJSON.factory);
        ModuleOutputRegistry.register(ResultSetLang.RS_JSON, new OutputWriterJSONPassage());

        // FusekiModules.add(new SageModule());

        FusekiServer.Builder serverBuilder = FusekiServer.create()
                // .parseConfigFile("configurations/sage.ttl") // TODO let it be a configuration file
                .port(port)
                .enablePing(true)
                .enableCompact(true)
                // .enableCors(true)
                .enableStats(true)
                .enableTasks(true)
                .enableMetrics(true)
                .numServerThreads(1, 10)
                // .loopback(false)
                .serverAuthPolicy(Auth.ANY_ANON) // Anyone can access the server
                .addProcessor("/$/server", new ActionServerStatus())
                //.addProcessor("/$/datasets/*", new ActionDatasets())
                .registerOperation(PassageOperation.Passage, new SPARQL_QueryDataset())
                .addDataset(name, dataset.asDatasetGraph()) // register the dataset
                .addEndpoint(name, "passage", PassageOperation.Passage)
                // .auth(AuthScheme.BASIC)
                // .addEndpoint(name, name+"/meow", SageOperation.Sage, Auth.ANY_ANON)
        ;

        if (Objects.nonNull(ui)) { // add UI if need be
            serverBuilder.staticFileBase(ui);
        }

        return serverBuilder.build();

    }
}
