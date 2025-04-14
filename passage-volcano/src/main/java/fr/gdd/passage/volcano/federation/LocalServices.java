package fr.gdd.passage.volcano.federation;

import com.google.common.collect.Multiset;
import fr.gdd.passage.commons.generics.BackendBindings;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.HashMap;
import java.util.Map;

/**
 * Some registered URI are redirected locally instead of calling
 * a remote service. Among others, this is useful to facilitate testing,
 * but also to provide home-made services.
 * *
 * TODO consider extending this to any service, be it local or remote.
 *      this could prove useful to maintain statistics about endpoints,
 *      for instance, their average latency.
 * TODO also, this would set the http builder, and uniformize the RuntimeException
 *      that would then be handled more easily
 */
public class LocalServices {

    Map<String, ILocalService> map = new HashMap<>();

    public LocalServices register(String uri, ILocalService service) {
        map.put(uri, service);
        return this;
    }

    public LocalServices unregister(String uri) {
        map.remove(uri);
        return this;
    }

    /**
     * @param uri The uri to check.
     * @return True if the uri is explicitly registered by the service handler.
     */
    public boolean contains(String uri) {
        return map.containsKey(uri);
    }

    /**
     * Tries to dispatch the query to another local service. Careful: if a service
     * is registered here, the priority is to get it from here, not from remote.
     * @param uri The unique identifier of the service.
     * @param query The SPARQL query to process.
     * @param args The additional arguments for the service to process.
     * @return The resulting bindings along with optional metadata.
     * @throws RuntimeException Some runtime exception. This is kept general as it can return many different
     *         kind of exceptions depending on the service.
     */
    public Pair<Multiset<Binding>, Map<String, ?>> query(String uri, Op query, BackendBindings<?,?> input, Map<String, ?> args) throws RuntimeException {
        ILocalService service = map.get(uri);
            return input.isEmpty() ?
                    service.query(query, args):
                    service.query(OpJoin.create(input.asValues(), query), args);
    }

    public Pair<Multiset<Binding>, Map<String, ?>> query(String uri, Op query, BackendBindings<?,?> input) throws RuntimeException {
        return query(uri, query, input, new HashMap<>());
    }

    public Pair<Multiset<Binding>, Map<String, ?>> query(String uri, Op query) throws RuntimeException {
        return query(uri, query, new BackendBindings<>(), new HashMap<>());
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        LocalServices localServices = new LocalServices();
        localServices.map = new HashMap<>(this.map);
        return localServices;
    }
}
