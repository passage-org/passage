package fr.gdd.raw.tdb2;

import fr.gdd.passage.commons.exceptions.NotFoundException;
import fr.gdd.passage.commons.interfaces.Backend;
import fr.gdd.passage.commons.interfaces.BackendIterator;
import fr.gdd.passage.commons.iterators.BackendLazyIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.sys.TDBInternal;

import java.util.Objects;


/**
 * TDB2 Jena Backend implementation of the interface `Backend`.
 **/
public class JenaBackend implements Backend<NodeId, Node> {

    Dataset dataset;
    DatasetGraphTDB graph;

    NodeTupleTable nodeQuadTupleTable;
    NodeTupleTable nodeTripleTupleTable;
    NodeTable  nodeTripleTable;
    NodeTable  nodeQuadTable;
    PreemptTupleTable preemptableTripleTupleTable;
    PreemptTupleTable preemptableQuadTupleTable;


    public JenaBackend(final String path) {
        dataset = TDB2Factory.connectDataset(path);
        graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        if (!dataset.isInTransaction()) {
            graph.begin();  // opened in at creation
        }
        loadDataset();
    }

    public JenaBackend(final Dataset dataset) {
        this.dataset = dataset;
        graph = TDBInternal.getDatasetGraphTDB(this.dataset);
        if (!dataset.isInTransaction()) {
            graph.begin();  // opened in at creation
        }
        loadDataset();
    }

    public JenaBackend(final DatasetGraph datasetGraph) {
        graph = TDBInternal.getDatasetGraphTDB(datasetGraph);
        if (!graph.isInTransaction()) {
            graph.begin();  // opened in at creation
        }
        loadDataset();
    }

    private void loadDataset() {
        nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        nodeTripleTable = nodeTripleTupleTable.getNodeTable();
        nodeQuadTable  = nodeQuadTupleTable.getNodeTable();
        preemptableTripleTupleTable = new PreemptTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptableQuadTupleTable   = new PreemptTupleTable(nodeQuadTupleTable.getTupleTable());
    }

    /**
     * Needs to be closed this one.
     */
    public void close() { graph.end(); }


    /* ****************************************************************************************** */

    @Override
    public BackendIterator<NodeId, Node> search(final NodeId s, final NodeId p, final NodeId o) {
        Tuple<NodeId> pattern = TupleFactory.tuple(s, p, o);
        return new BackendLazyIterator<>(this, preemptableTripleTupleTable.preemptFind(pattern));
    }

    @Override
    public BackendIterator<NodeId, Node> search(final NodeId s, final NodeId p, final NodeId o, final NodeId c) {
        Tuple<NodeId> pattern = TupleFactory.tuple(c, s, p, o);
        return new BackendLazyIterator<>(this, preemptableQuadTupleTable.preemptFind(pattern));
    }

    @Override
    public NodeId getId(final String value, final int... code) throws NotFoundException {
        Node node = NodeFactoryExtra.parseNode(value);
        NodeId id = nodeTripleTable.getNodeIdForNode(node);
        if (NodeId.isDoesNotExist(id)) {
            id = nodeQuadTable.getNodeIdForNode(node);
            if (NodeId.isDoesNotExist(id)) {
                throw new NotFoundException(value);
            }
        }
        return id;
    }

    @Override
    public String getString(final NodeId id, final int... code) throws NotFoundException {
        Node node = nodeTripleTable.getNodeForNodeId(id);
        if (Objects.isNull(node)) {
            node = nodeQuadTable.getNodeForNodeId(id);
            if (Objects.isNull(node)) {
                throw new NotFoundException(String.format("Id of %s does not exist.", id.toString()));
            }
        }
        return node.toString();
    }

    public Node getValue(final NodeId id, final int... code) throws NotFoundException {
        Node node = nodeTripleTable.getNodeForNodeId(id);
        if (Objects.isNull(node)) {
            node = nodeQuadTable.getNodeForNodeId(id);
            if (Objects.isNull(node)) {
                throw new NotFoundException(id.toString());
            }
        }
        return node;
    }

    @Override
    public NodeId any() {
        return NodeId.NodeIdAny;
    }

    /* **************************************************************************************** */

    /**
     * Convenience function that gets the id from the node, looking in both tables.
     */
    @Override
    public NodeId getId(final Node node, int... code) throws NotFoundException {
        NodeId id = nodeTripleTable.getNodeIdForNode(node);
        if (NodeId.isDoesNotExist(id)) {
            id = nodeQuadTable.getNodeIdForNode(node);
            if (NodeId.isDoesNotExist(id)) {
                throw new NotFoundException(node.toString());
            }
        }
        return id;
    }

    public NodeTable getNodeTripleTable() {
        return nodeTripleTable;
    }

    public NodeTable getNodeQuadTable() {
        return nodeQuadTable;
    }
}
