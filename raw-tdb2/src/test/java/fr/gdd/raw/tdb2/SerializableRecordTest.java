package fr.gdd.raw.tdb2;

import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.raw.tdb2.datasets.TDB2InMemoryDatasetsFactory;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.ext.xerces.impl.dv.util.Base64;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Single test to check if the serialization works properly. Will be useful
 * when sending the data through a network.
 * */
class SerializableRecordTest {

    @Test
    public void serialize_then_deserialize_a_record() {
        Dataset dataset = TDB2InMemoryDatasetsFactory.graph3();

        JenaBackend backend = new JenaBackend(dataset);
        NodeId predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        NodeId any = backend.any();

        BackendIterator<NodeId, Node> it = backend.search(any, predicate, any);
        it.next();
        it.next();
        SerializableRecord sr = it.current();
        assertEquals(2, sr.getOffset());

        byte[] serialized = SerializationUtils.serialize(sr);
        String encoded = Base64.encode(serialized);
        byte[] decoded = Base64.decode(encoded);
        SerializableRecord deserialized = SerializationUtils.deserialize(decoded);

        assertEquals(0, Bytes.compare(sr.getRecord().getKey(), deserialized.getRecord().getKey()));
        assertEquals(sr.getOffset(), deserialized.getOffset());

        TDBInternal.expel(dataset.asDatasetGraph());
    }

}