package fr.gdd.passage.commons.datasets;

import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class DatasetAndQueriesBuilder<ID,VALUE,SKIP extends Serializable> {

    List<URI> urisToDownloadTheDataset = List.of(); // might need multiple files to join
    List<URI> urisToDownloadTheQueries = List.of(); // might need multiple directories

    Path pathToDataset = Path.of("./");
    List<Path> pathsToQueries = List.of(Path.of("./"));

    Boolean cleanAfterDownload = true;

    BackendFactory<ID,VALUE,SKIP> factory;

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
        Backend<ID,VALUE> backend = factory.get(pathToDataset);
        if (pathsToQueries.stream().anyMatch(p -> p.toFile().isFile())) {
            throw new IllegalArgumentException("The query path appears to be a file while it should be a directory.");
        }
        pathsToQueries.stream().filter(p -> !p.toFile().isDirectory()).forEach(p -> {
            try { Files.createDirectories(p);  } catch (IOException e) { throw new RuntimeException(e); }
        });

        // read inside the directory if it has the queries
        for (Path pathToQueries : pathsToQueries) {
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
        }

        Map<String, Op> name2query = new HashMap<>();
        for (Path pathToQueries : pathsToQueries) {
            File[] queryFiles = pathToQueries.toFile().listFiles((dir, name) -> name.endsWith(".sparql"));
            Map<String, Op> name2queryOfDirectotry = Arrays.stream(queryFiles).map(f -> {
                try {
                    Op query = Algebra.compile(QueryFactory.create(Files.readString(f.toPath(), StandardCharsets.UTF_8)));
                    return Map.entry(f.getName(), query);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            name2query.putAll(name2queryOfDirectotry);
        }

        return new DatasetAndQueries<>(backend, name2query, Map.of()); // TODO ground truth
    }

    /* ********************************************************************* */

    public DatasetAndQueriesBuilder<ID,VALUE,SKIP> setBackendKind(BackendFactory<ID,VALUE,SKIP> factory) {
        this.factory = factory;
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE,SKIP> addUriToDownloadTheDataset(String uriAsString) {
        urisToDownloadTheDataset.add(URI.create(uriAsString));
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE,SKIP> addUriToDownloadTheQueries(String uriAsString) {
        urisToDownloadTheQueries.add(URI.create(uriAsString));
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE,SKIP> cleanAfterDownload() {
        this.cleanAfterDownload = true;
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE,SKIP> dirtyAfterDownload() {
        this.cleanAfterDownload = false;
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE,SKIP> setPathToDataset(String pathToDataset) {
        this.pathToDataset = Path.of(pathToDataset);
        return this;
    }

    public DatasetAndQueriesBuilder<ID,VALUE,SKIP> addPathToQueries(String pathToQueries) {
        this.pathsToQueries.add(Path.of(pathToQueries));
        return this;
    }
}
