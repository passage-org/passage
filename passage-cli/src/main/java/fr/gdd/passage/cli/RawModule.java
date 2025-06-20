package fr.gdd.passage.cli;

import org.apache.jena.fuseki.main.sys.FusekiModule;

import java.util.Objects;

public class RawModule extends PassageModule implements FusekiModule {

//    public final static ArgDecl argDatabase = new ArgDecl(ArgDecl.HasValue, "database");
//    public final static ArgDecl argThreads = new ArgDecl(ArgDecl.NoValue, "threads");
//    public final static ArgDecl argTimeout = new ArgDecl(ArgDecl.NoValue, "timeout");
//    public final static ArgDecl argMaxResults = new ArgDecl(ArgDecl.NoValue, "maxResults");

    // Might not be worth extending passage module if it leads to weird stuff like this class cast ...
    public static RawModule create() { if (Objects.isNull(singleton)) singleton = new RawModule(); return (RawModule) singleton; }

    public RawModule() {
        super();
    }

    @Override
    public String name() { return "Raw (configured by a regular Fuseki --config=FILE)"; }
}
