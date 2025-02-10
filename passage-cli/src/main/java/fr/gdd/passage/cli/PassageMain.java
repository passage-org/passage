package fr.gdd.passage.cli;

import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.cmds.FusekiMain;

public class PassageMain extends FusekiMain {

    public final static ArgDecl argDatabase = new ArgDecl(ArgDecl.HasValue, "--database");
    public final static ArgDecl argThreads = new ArgDecl(ArgDecl.NoValue, "--threads");
    public final static ArgDecl argTimeout = new ArgDecl(ArgDecl.NoValue, "--timeout");
    public final static ArgDecl argMaxResults = new ArgDecl(ArgDecl.NoValue, "--maxResults");

    public PassageMain() {
        argsSetup();
    }

    public PassageMain(String[] args) {
        super(args);
        argsSetup();
    }

    private void argsSetup () {
        getUsage().startCategory("Passage");
        add(argDatabase, "--database=FILE", "The path to your blazegraph|hdt database");
        add(argThreads, "--threads=1", "Number of threads dedicated to execute the query");
        add(argTimeout, "--timeout", "Timeout before the query execution is stopped (value in ms)");
        add(argMaxResults, "--maxResults", "Maximum number of results to return before the query execution is stopped");
    }

}
