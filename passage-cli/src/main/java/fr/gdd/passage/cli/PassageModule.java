package fr.gdd.passage.cli;

import fr.gdd.passage.cli.server.PassageOutputWriterJSON;
import fr.gdd.passage.cli.server.PassageQueryEngine;
import fr.gdd.passage.cli.vocabularies.VocabBlazegraph;
import fr.gdd.passage.commons.io.ExtensibleRowSetWriterJSON;
import fr.gdd.passage.commons.io.ModuleOutputRegistry;
import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.cmd.ArgModuleGeneral;
import org.apache.jena.cmd.CmdGeneral;
import org.apache.jena.fuseki.main.cmds.ServerArgs;
import org.apache.jena.fuseki.main.sys.FusekiModule;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;

import java.util.Objects;

public class PassageModule implements FusekiModule {

    private static PassageModule singleton;

    public final static ArgDecl argDatabase = new ArgDecl(ArgDecl.HasValue, "database");
    public final static ArgDecl argThreads = new ArgDecl(ArgDecl.NoValue, "threads");
    public final static ArgDecl argTimeout = new ArgDecl(ArgDecl.NoValue, "timeout");
    public final static ArgDecl argMaxResults = new ArgDecl(ArgDecl.NoValue, "maxResults");

    public static PassageModule create() { if (Objects.isNull(singleton)) singleton = new PassageModule(); return singleton; }

    private PassageModule() {
        VocabBlazegraph.init();
        // set globally but the dedicated writer only comes into
        // play when some variables exist in the execution context.
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, ExtensibleRowSetWriterJSON.factory);
        ModuleOutputRegistry.register(ResultSetLang.RS_JSON, new PassageOutputWriterJSON());
        PassageQueryEngine.register();
    }

    @Override
    public String name() { return "Passage Module"; }

    @Override
    public void serverArgsModify(CmdGeneral fusekiCmd, ServerArgs serverArgs) {
        fusekiCmd.getUsage().startCategory(name());
        ArgModuleGeneral argModule = cmdLine -> {
            cmdLine.add(argDatabase, "--database=FILE", "The path to your blazegraph|hdt database");
            cmdLine.add(argThreads, "--threads=1", "Number of threads dedicated to execute the query");
            cmdLine.add(argTimeout, "--timeout", "Timeout before the query execution is stopped (value in ms)");
            cmdLine.add(argMaxResults, "--maxResults", "Maximum number of results to return before the query execution is stopped");
        };
        argModule.registerWith(fusekiCmd);
    }

}
