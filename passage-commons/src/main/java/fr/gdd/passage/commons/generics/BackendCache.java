package fr.gdd.passage.commons.generics;

import fr.gdd.passage.commons.interfaces.Backend;
import org.apache.jena.graph.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Caches the values that are commonly used in the whole query. Since
 * the query is parsed using Jena, the base value is a `Node` that
 * comes from the logical parsed plan.
 */
public class BackendCache<ID,VALUE> {

    final Backend<ID,VALUE,?> backend;

    Map<Node, ID> node2id = new HashMap<>();

    public BackendCache(Backend<ID,VALUE,?> backend) {
        this.backend = backend;
    }

    public ID getId(Node node, Integer spoc) {
        ID id = node2id.get(node);

        if (Objects.isNull(id)) {
            if (node.isURI()) { // ugly…
                id = backend.getId("<" + node + ">", spoc);
            } else {
                id = backend.getId(node.toString(), spoc);
            }
            node2id.put(node, id);
        }

        return id;
    }

    public ID getId(Node node) {
        return node2id.get(node);
    }

    /**
     * Register in the cache a node that is already known by ID.
     * Useful for initializing the cache of subquery where bound variables
     * have been added.
     * @return this, for convenience.
     */
    public BackendCache<ID,VALUE> register(Node node, ID id) {
        node2id.put(node, id); // we don't check anything
        return this;
    }

    /**
     * Copy the content of the other cache into this one.
     * @param otherCache The cache to copy.
     * @return this, for convenience.
     */
    public BackendCache<ID,VALUE> copy(BackendCache<ID,VALUE> otherCache) {
        this.node2id.putAll(otherCache.node2id);
        return this;
    }
}
