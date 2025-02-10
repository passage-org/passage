package fr.gdd.passage.cli;

import fr.gdd.passage.volcano.PassageConstants;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.assemblers.AssemblerGroup;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.system.Vocab;

public class VocabBlazegraph {

    // mostly add the keyword DatasetBlazegraph to those accepted
    public static final Resource tDatasetBlazegraph = Vocab.type(PassageConstants.systemVarNS, "DatasetBlazegraph");
    public static final Property pLocation          = Vocab.property(PassageConstants.systemVarNS, "location");
    public static final Property pUnionDefaultGraph = Vocab.property(PassageConstants.systemVarNS, "unionDefaultGraph");

    private static boolean initialized = false;

    static { init(); }

    static public synchronized void init() {
        if ( initialized )
            return;
        registerWith(Assembler.general);
        initialized = true;
    }

    static void registerWith(AssemblerGroup g) {
        AssemblerUtils.registerDataset(tDatasetBlazegraph, new DatasetAssemblerBlazegraph());
    }
}
