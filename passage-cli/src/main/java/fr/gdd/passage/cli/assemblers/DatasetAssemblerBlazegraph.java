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
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.assembler.AssemblerUtils;
import org.apache.jena.sparql.core.assembler.DatasetAssembler;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sys.JenaSystem;

import java.util.Objects;

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
        // We don't use DatabaseMgr to create the DatasetGraph, since the
        // Apache Jena's machinery could confuse it for the actual dataset
        // and use TDB2 on it.
        DatasetGraph dsg = DatasetGraphFactory.create(); // empty as we do not use this abstraction

        // #1 retrieve the actual backend
        try {
            exactlyOneProperty(root, PassageVocabulary.location); // <= throws on false
            String path2database = getStringValue(root, PassageVocabulary.location);
            BlazegraphBackend backend = (BlazegraphBackend) manager.addBackend(path2database, new BlazegraphBackendFactory());
            dsg.getContext().set(BackendConstants.BACKEND, backend);
        } catch (Exception e) {
            throw e;
        }

        // #2 the engine is defined with the backend for now (TODO) engine should have their own assembler
        // #A get the engine kind
        Resource lEngine = getUniqueResource(root, PassageVocabulary.engine);
        if (Objects.isNull(lEngine) || lEngine.equals(PassageVocabulary.PassageEngine)) {
            QC.setFactory(dsg.getContext(), new PassageOpExecutorFactory()); // is also the default if none is given
        } else if (lEngine.equals(PassageVocabulary.RawEngine)) {
            throw new UnsupportedOperationException("Raw is not supported yet.");
        } else {
            throw new AssemblerException(root, "No valid engine given"); // unknown engine
        }

        // #B get its build argument
        Literal lTimeout = getUniqueLiteral(root, PassageVocabulary.timeout); // default unset
        if (Objects.nonNull(lTimeout)) { dsg.getContext().set(PassageConstants.TIMEOUT, lTimeout.getLong()); }
        Literal lForceOrder = getUniqueLiteral(root, PassageVocabulary.force_order); // default false
        dsg.getContext().set(PassageConstants.FORCE_ORDER, !Objects.isNull(lForceOrder) && lForceOrder.getBoolean());
        Literal lParallel = getUniqueLiteral(root, PassageVocabulary.parallel); // default 1
        dsg.getContext().set(PassageConstants.MAX_PARALLELISM, Objects.isNull(lParallel) ? 1 : lParallel.getInt());

        AssemblerUtils.mergeContext(root, dsg.getContext());
        return dsg;
    }
}
