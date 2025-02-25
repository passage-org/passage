package fr.gdd.passage.cli.assemblers;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.blazegraph.BlazegraphBackendFactory;
import fr.gdd.passage.cli.server.PassageOpExecutorFactory;
import fr.gdd.passage.cli.vocabularies.PassageVocabulary;
import fr.gdd.passage.commons.generics.BackendConstants;
import fr.gdd.passage.commons.generics.BackendManager;
import fr.gdd.passage.volcano.PassageConstants;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.core.assembler.DatasetAssembler;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.tdb2.assembler.VocabTDB2;

import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.getStringValue;

public class DatasetAssemblerBlazegraph extends DatasetAssembler {
    // This is not a NamedDatasetAssembler
    // Sharing is done by "same location" and must be system wide (not just assemblers).
    // In-memory TDB2 dataset can use named memory locations e.g. "--mem--/NAME" TODO <--

    // just in case where a dataset path would be mentioned multiple times, we create a
    // manager that ensures uniqueness in opening files.
    private final static BackendManager manager = new BackendManager();

    static { JenaSystem.init(); }

    @Override
    public DatasetGraph createDataset(Assembler a, Resource root) {
        return make(a, root);
    }

    public static DatasetGraph make(Assembler a, Resource root) {
        try {
            // actually returns true or throws an exception.
            exactlyOneProperty(root, PassageVocabulary.location);
        } catch (Exception e) {
            throw new AssemblerException(root, "No location given");
        }

        // TODO need a connection manager for when the dataset is referenced multiple times
        String path2database = getStringValue(root, PassageVocabulary.location);
        // we don't use DatabaseMgr to create the DatasetGraph, since the
        // Apache Jena's machinery could confuse it for the actual dataset
        // and use TDB2 on it.
        DatasetGraph dsg = DatasetGraphFactory.create(); // empty as we do not use this abstraction

        // TODO put this as argument, in the dataset or in the service?
        dsg.getContext().set(PassageConstants.TIMEOUT, 1000000L);
        dsg.getContext().set(PassageConstants.FORCE_ORDER, false);
        dsg.getContext().set(PassageConstants.MAX_PARALLELISM, 1);

        try {
            BlazegraphBackend backend = (BlazegraphBackend) manager.addBackend(path2database, new BlazegraphBackendFactory());
            dsg.getContext().set(BackendConstants.BACKEND, backend);
        } catch (Exception e) {
            throw new AssemblerException(root, e.getMessage());
        }

        if ( root.hasProperty(VocabTDB2.pUnionDefaultGraph) ) {
            Node b = root.getProperty(VocabTDB2.pUnionDefaultGraph).getObject().asNode();
            NodeValue nv = NodeValue.makeNode(b);
            if ( nv.isBoolean() )
                dsg.getContext().set(Symbol.create(VocabTDB2.pUnionDefaultGraph.toString()), nv.getBoolean());
            else
                Log.warn(DatasetAssemblerBlazegraph.class, "Failed to recognize value for union graph setting (ignored): " + b);
        }

        QC.setFactory(dsg.getContext(), new PassageOpExecutorFactory()); // we fix the executor for this dataset

        AssemblerUtils.mergeContext(root, dsg.getContext());
        return dsg;
    }
}
