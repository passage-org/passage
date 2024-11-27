package fr.gdd.passage.blazegraph.datasets;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import fr.gdd.passage.blazegraph.BlazegraphBackend;
import fr.gdd.passage.commons.datasets.DatasetAndQueries;
import fr.gdd.passage.commons.datasets.DatasetAndQueriesBuilder;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Provides datasets and queries from notorious RDF benchmarks.
 */
public class BlazegraphBenchmarksFactory {

    public static DatasetAndQueries<IV, BigdataValue> getWatDiv10M () throws IOException {
        return new DatasetAndQueriesBuilder<IV,BigdataValue,Long>()
                .setPathToDataset("/Users/nedelec-b-2/Desktop/Projects/temp/watdiv10m-blaze/watdiv10M.jnl")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12744414/files/watdiv10m-blaze.zip")
                .addPathToQueries("/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/watdiv10m-sage-ordered/")
                .setBackendKind(BlazegraphBenchmarksFactory::create)
                .build();
    }

    public static DatasetAndQueries<IV, BigdataValue> getWDBench () throws IOException {
        return new DatasetAndQueriesBuilder<IV,BigdataValue,Long>()
                .setPathToDataset("/Users/nedelec-b-2/Desktop/Projects/temp/wdbench-blaze/wdbench-blaze.jnl")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-aa")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-ab")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-ac")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-ad")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-ae")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-af")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-ag")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-ah")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12510935/files/wdbench-blaze.zip-part-ai")
                .addUriToDownloadTheDataset("https://zenodo.org/records/12511050/files/wdbench-blaze.zip-part-aj")
                .addPathToQueries("/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench-multiple-tps/")
                .addPathToQueries("/Users/nedelec-b-2/Desktop/Projects/sage-jena-benchmarks/queries/wdbench-opts")
                .setBackendKind(BlazegraphBenchmarksFactory::create)
                .build();
    }

    public static DatasetAndQueries<IV, BigdataValue> getFedShop () throws IOException {
        return new DatasetAndQueriesBuilder<IV,BigdataValue,Long>()
                .setPathToDataset("/Users/nedelec-b-2/Desktop/Projects/temp/fedshop-blaze/fedshop.jnl")
                .addUriToDownloadTheDataset("https://zenodo.org/records/14224920/files/fedshop.jnl")
                .setBackendKind(BlazegraphBenchmarksFactory::create)
                .build();
    }

    /* ********************************************************************* */

    private static BlazegraphBackend create(Path path) {
        try {
            return new BlazegraphBackend(path.toString());
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }
}
