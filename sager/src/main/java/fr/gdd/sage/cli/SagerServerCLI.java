package fr.gdd.sage.cli;

import fr.gdd.sage.blazegraph.BlazegraphBackend;
import fr.gdd.sage.interfaces.Backend;
import fr.gdd.sage.sager.SagerConstants;
import fr.gdd.sage.sager.SagerOpExecutorFactory;
import fr.gdd.sage.sager.writers.ExtensibleRowSetWriterJSON;
import fr.gdd.sage.sager.writers.ModuleOutputRegistry;
import fr.gdd.sage.sager.writers.OutputWriterJSONSage;
import org.apache.jena.fuseki.auth.Auth;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.mgt.ActionServerStatus;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.fuseki.server.ServerConst;
import org.apache.jena.fuseki.servlets.ActionService;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.engine.main.QC;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Command Line Interface of the Fuseki server running Sage.
 */
@CommandLine.Command(
        name = "sage-server",
        version = "0.0.4",
        description = "A server for preemptive SPARQL query processing!",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class SagerServerCLI {

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
        SagerServerCLI serverOptions = new SagerServerCLI();
        new CommandLine(serverOptions).parseArgs(args);

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
     * Build a Sage fuseki server.
     * @param backend A backend that supports preemption.
     * @param ui The path to the ui.
     * @return A fuseki server not yet running.
     */
    static FusekiServer buildServer(String name, Backend<?, ?, Long> backend,
                                    Long timeout,
                                    Integer port,
                                    String ui) {
        // wraps our database inside a standard but empty dataset.
        Dataset dataset = DatasetFactory.create(); // TODO double check if it's alright
        dataset.getContext().set(SagerConstants.BACKEND, backend);
        dataset.getContext().set(SagerConstants.TIMEOUT, timeout);
        QC.setFactory(dataset.getContext(), new SagerOpExecutorFactory());

        // set globally but the dedicated writter of sage only comes into
        // play when some variables exist in the execution context.
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, ExtensibleRowSetWriterJSON.factory);
        ModuleOutputRegistry.register(ResultSetLang.RS_JSON, new OutputWriterJSONSage());

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
                .registerOperation(SageOperation.Sage, new SPARQL_QueryDataset())
                .addDataset(name, dataset.asDatasetGraph()) // register the dataset
                .addEndpoint(name, "sage", SageOperation.Sage)
                // .auth(AuthScheme.BASIC)
                // .addEndpoint(name, name+"/meow", SageOperation.Sage, Auth.ANY_ANON)
        ;

        if (Objects.nonNull(ui)) { // add UI if need be
            serverBuilder.staticFileBase(ui);
        }

        return serverBuilder.build();

    }
}
