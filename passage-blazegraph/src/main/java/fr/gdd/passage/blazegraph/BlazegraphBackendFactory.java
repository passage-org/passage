package fr.gdd.passage.blazegraph;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendFactory;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import java.nio.file.Path;

public class BlazegraphBackendFactory implements BackendFactory<IV, BigdataValue, Long> {

    @Override
    public Backend<IV, BigdataValue> get(Path path) {
        try {
            return new BlazegraphBackend(path.toString());
        } catch (SailException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

}
