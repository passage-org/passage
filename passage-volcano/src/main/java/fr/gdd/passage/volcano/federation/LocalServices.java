package fr.gdd.passage.volcano.federation;

import com.google.common.collect.Multiset;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.sparql.engine.binding.Binding;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Some registered URI are redirected locally instead of calling
 * a remote service. Among others, this is useful to facilitate testing,
 * but also to provide home-made services.
 */
public class LocalServices {

    Map<String, Supplier<ILocalService>> map = new HashMap<>();

    public LocalServices register(String uri, Supplier<ILocalService> supplier) {
        map.put(uri, supplier);
        return this;
    }

    public LocalServices unregister(String uri) {
        map.remove(uri);
        return this;
    }

    public Pair<Multiset<Binding>, Map<String, ?>> query(String uri, String query, String... args) {
        Supplier<ILocalService> supplier = map.get(uri);
        return supplier.get().query(query, args);
    }
}
