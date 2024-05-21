package fr.gdd.sage.generics;

import fr.gdd.sage.interfaces.Backend;
import org.apache.jena.graph.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CacheId<ID,VALUE> {

    final Backend<ID,VALUE,?> backend;

    Map<Node, ID> node2id = new HashMap<>();

    public CacheId(Backend<ID,VALUE,?> backend) {
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
}
