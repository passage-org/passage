package fr.gdd.passage.cli;

import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.generics.BackendConstants;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.core.assembler.DatasetAssembler;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.tdb2.DatabaseMgr;

import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.getStringValue;

public class DatasetAssemblerBlazegraph extends DatasetAssembler
{
    // This is not a NamedDatasetAssembler
    // Sharing is done by "same location" and must be system wide (not just assemblers).
    // In-memory TDB2 dataset can use named memory locations e.g. "--mem--/NAME" TODO <--

    static { JenaSystem.init(); }

    @Override
    public DatasetGraph createDataset(Assembler a, Resource root) {
        return make(a, root);
    }

    public static DatasetGraph make(Assembler a, Resource root) {
        try {
            // actually returns true or throws an exception.
            exactlyOneProperty(root, VocabBlazegraph.pLocation);
        } catch (Exception e) {
            throw new AssemblerException(root, "No location given");
        }

        // TODO need a connection manager for when the dataset is referenced multiple times
        String path2database = getStringValue(root, VocabBlazegraph.pLocation);
        DatasetGraph dsg = DatabaseMgr.createDatasetGraph(); // empty as we do not use this abstraction

        try {
            BlazegraphBackend backend = new BlazegraphBackend(path2database);
            dsg.getContext().set(BackendConstants.BACKEND, backend);
        } catch (Exception e) {
            throw new AssemblerException(root, e.getMessage());
        }

        if ( root.hasProperty(VocabBlazegraph.pUnionDefaultGraph) ) {
            Node b = root.getProperty(VocabBlazegraph.pUnionDefaultGraph).getObject().asNode();
            NodeValue nv = NodeValue.makeNode(b);
            if ( nv.isBoolean() )
                dsg.getContext().set(Symbol.create(VocabBlazegraph.pUnionDefaultGraph.toString()), nv.getBoolean());
            else
                Log.warn(DatasetAssemblerBlazegraph.class, "Failed to recognize value for union graph setting (ignored): " + b);
        }

        AssemblerUtils.mergeContext(root, dsg.getContext());
        return dsg;
    }
}
