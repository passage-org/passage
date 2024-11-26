package fr.gdd.passage.databases.persistent;

import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendFactory;
import fr.gdd.passage.databases.inmemory.IM4Blazegraph;
import it.unimi.dsi.fastutil.ints.Int2ByteMap;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DatasetAndQueriesBuilder<ID,VALUE> {

    List<URI> urisToDownloadTheDataset = List.of(); // might need multiple files to join
    List<URI> urisToDownloadTheQueries = List.of(); // might need multiple directories

    Path pathToDataset = Path.of("./");
    Path pathToQueries = Path.of("./");

    Boolean cleanAfterDownload = true;

    BackendFactory<ID,VALUE,?> factory;

    public DatasetAndQueriesBuilder() {}

    public DatasetAndQueries<ID,VALUE> build() throws IOException {
        if (!pathToDataset.toFile().exists()) {
            if (urisToDownloadTheDataset.isEmpty()) {
                throw new IllegalArgumentException("The dataset cannot be found nor downloaded.");
            }
            // TODO download
            // TODO join if multiple files
            throw new UnsupportedOperationException("Download of dataset.");
            // TODO cleanup if need be
        }
        Backend<ID,VALUE,?> backend = factory.get(pathToDataset);
        if (pathToQueries.toFile().isFile()) {
            throw new IllegalArgumentException("The query path appears to be a file while it should be a directory.");
        }
        if (!pathToQueries.toFile().isDirectory()) {
            Files.createDirectories(pathToQueries);
        }

        // read inside the directory if it has the queries
        File[] queryFiles = pathToQueries.toFile().listFiles((dir, name) -> name.endsWith(".sparql"));

        if (Objects.isNull(queryFiles) || queryFiles.length == 0) {
            if (urisToDownloadTheDataset.isEmpty()) {
                throw new IllegalArgumentException("The dataset cannot be found nor downloaded.");
            } else {
                // TODO download
                throw new UnsupportedOperationException("Download of queries.");
                // TODO cleanup if need be
            }
        }

        queryFiles = pathToQueries.toFile().listFiles((dir, name) -> name.endsWith(".sparql"));
        Map<String, Op> name2query = Arrays.stream(queryFiles).map(f -> {
            try {
                Op query = Algebra.compile(QueryFactory.create(Files.readString(f.toPath(), StandardCharsets.UTF_8)));
                return Map.entry(f.getName(), query);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new DatasetAndQueries<>(backend, name2query, Map.of()); // TODO ground truth
    }

    /* ********************************************************************* */

    public DatasetAndQueriesBuilder<ID,VALUE> setBackendKind(BackendFactory<ID,VALUE,?> factory) {
        this.factory = factory;
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE> addUriToDownloadTheDataset(String uriAsString) {
        urisToDownloadTheDataset.add(URI.create(uriAsString));
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE> addUriToDownloadTheQueries(String uriAsString) {
        urisToDownloadTheQueries.add(URI.create(uriAsString));
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE> cleanAfterDownload() {
        this.cleanAfterDownload = true;
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE> dirtyAfterDownload() {
        this.cleanAfterDownload = false;
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE> setPathToDataset(String pathToDataset) {
        this.pathToDataset = Path.of(pathToDataset);
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE> setPathToQueries(String pathToQueries) {
        this.pathToQueries = Path.of(pathToQueries);
        return this;
    }
}
