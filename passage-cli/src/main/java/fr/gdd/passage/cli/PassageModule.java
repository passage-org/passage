package fr.gdd.passage.cli;

import fr.gdd.passage.cli.assemblers.DatasetAssemblerBlazegraph;
import fr.gdd.passage.cli.operations.SPARQL_QueryDatasetWithHeaders;
import fr.gdd.passage.cli.server.PassageOutputWriterJSON;
import fr.gdd.passage.cli.server.PassageQueryEngine;
import fr.gdd.passage.cli.vocabularies.PassageVocabulary;
import fr.gdd.passage.commons.io.ExtensibleRowSetWriterJSON;
import fr.gdd.passage.commons.io.ModuleOutputRegistry;
import org.apache.jena.cmd.CmdGeneral;
import org.apache.jena.fuseki.main.cmds.ServerArgs;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.fuseki.server.Operation;
import org.apache.jena.fuseki.server.OperationRegistry;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;

import java.util.Objects;

public class PassageModule implements FusekiModule {

    private static PassageModule singleton;

//    public final static ArgDecl argDatabase = new ArgDecl(ArgDecl.HasValue, "database");
//    public final static ArgDecl argThreads = new ArgDecl(ArgDecl.NoValue, "threads");
//    public final static ArgDecl argTimeout = new ArgDecl(ArgDecl.NoValue, "timeout");
//    public final static ArgDecl argMaxResults = new ArgDecl(ArgDecl.NoValue, "maxResults");

    public static PassageModule create() { if (Objects.isNull(singleton)) singleton = new PassageModule(); return singleton; }

    public PassageModule() {
        // explicit call to avoid our own RS_JSON from getting erased by the default one.
        RowSetWriterRegistry.init();
        // set globally but the dedicated writer only comes into
        // play when some variables exist in the execution context.
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, ExtensibleRowSetWriterJSON.factory);
        ModuleOutputRegistry.register(ResultSetLang.RS_JSON, new PassageOutputWriterJSON());
        PassageQueryEngine.register();

        AssemblerUtils.addRegistered(PassageVocabulary.DatasetBlazegraph.getModel());
        AssemblerUtils.registerDataset(PassageVocabulary.DatasetBlazegraph, new DatasetAssemblerBlazegraph());

        // Not mandatory but cool feature enabled: reading user-defined args from the request.
        Operation queryWArgs = Operation.alloc(PassageVocabulary.query_w_args.asNode(),
                PassageVocabulary.query_w_args.getLocalName(),
                "SPARQL query operation that allows reading body's and header's arguments.");
        OperationRegistry.get().register(queryWArgs, new SPARQL_QueryDatasetWithHeaders());
    }

    @Override
    public String name() { return "Passage (configured by a regular Fuseki --config=FILE)"; }

    @Override
    public void serverArgsModify(CmdGeneral fusekiCmd, ServerArgs serverArgs) {
        fusekiCmd.getUsage().startCategory(name());

//        ArgModuleGeneral argModule = cmdLine -> {
//            cmdLine.add(argDatabase, "--database=FILE", "The path to your blazegraph|hdt database");
//            cmdLine.add(argThreads, "--threads=1", "Number of threads dedicated to execute the query");
//            cmdLine.add(argTimeout, "--timeout", "Timeout before the query execution is stopped (value in ms)");
//            cmdLine.add(argMaxResults, "--maxResults", "Maximum number of results to return before the query execution is stopped");
//        };
//         argModule.registerWith(fusekiCmd);
    }

}
