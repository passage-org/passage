package fr.gdd.passage.cli;

import org.apache.jena.cmd.TerminationException;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.main.cmds.FusekiMain;
import org.apache.jena.fuseki.main.sys.FusekiModules;
import org.apache.jena.tdb2.assembler.VocabTDB2;

/**
 * Entrypoint for the main CLI for the passage server which is a regular Fuseki server
 * extended with continuation capabilities.
 */
public class PassageServerCLI2 {

    public static void main(String[] args) {
        FusekiMain.addCustomisers(FusekiModules.create(PassageModule.create()));
        try {
            VocabTDB2.init();
            FusekiServer server = FusekiMain.builder(args).build();
            server.start();
        } catch (TerminationException e) {
            System.exit(e.returnCode); // regular quit instead of throwing an exception
        }
    }
}
