package fr.gdd.passage.cli;

import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.cmd.CmdGeneral;
import org.apache.jena.fuseki.main.cmds.FusekiMain;
import org.apache.jena.fuseki.main.cmds.ServerArgs;
import org.apache.jena.fuseki.main.sys.FusekiServerArgsCustomiser;

public class PassageServer extends FusekiMain {

    public PassageServer() {
        add(new ArgDecl(true, "meow", "meow"));
    }

}
