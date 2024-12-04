package fr.gdd.passage.commons.generics;

import com.google.common.collect.ConcurrentHashMultiset;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.atlas.lib.IdentityFinishCollector;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Hash on keys then compare pointers of keys. It's meant to increase search
 * speed using hash, then retrieve the exact value using pointers.
 *  * @param <K> Key type.
 *  * @param <V> Value type.
 */
public class ConcurrentPtrMap<K, V> {

    final ConcurrentHashMap<K, ConcurrentHashMultiset<ImmutablePair<K, V>>> map = new ConcurrentHashMap<>();
    int size = 0;

    public ConcurrentPtrMap<K, V> put (K key, V val) {
        ConcurrentHashMultiset<ImmutablePair<K, V>> values = map.putIfAbsent(key, ConcurrentHashMultiset.create());

        if (Objects.isNull(values)) { values = map.get(key); }

        var alreadyExists = values.stream().filter(p -> p.getLeft() == key).toList();
        if (alreadyExists.isEmpty()) { // not in the list of pairs, should add it
            values.add(new ImmutablePair<>(key, val));
            ++size;
        } else { // Already in the list of pairs, should replace it. Should not happen though
            values.remove(alreadyExists.getFirst()); // -1
            values.add(new ImmutablePair<>(key, val)); // +1 = 0
        }

        return this;
    }

    public ConcurrentPtrMap<K, V> remove (K key) {
        if (map.containsKey(key)) {
            ConcurrentHashMultiset<ImmutablePair<K, V>> values = map.get(key);
            values.removeIf(p -> p.getLeft() == key);
            --size;
            //            if (values.isEmpty()) {
            //                map.remove(key);
            //            }
        }
        return this;
    }

    public int size () {
        return this.size;
    }

    public V get (K key) {
        if (map.containsKey(key)) {
            ConcurrentHashMultiset<ImmutablePair<K, V>> values = map.get(key);
            List<ImmutablePair<K, V>> result = values.stream().filter(p -> p.getLeft() == key).toList();
            if (!result.isEmpty()) {
                return result.getFirst().getRight();
            }
        }
        return null;
    }

    public void computeIfPresent(K key, Consumer<ConcurrentHashMultiset<ImmutablePair<K,V>>> func) {
        map.putIfAbsent(key, ConcurrentHashMultiset.create()); // cannot be removed
        map.computeIfPresent(key, (k,v) -> { // then, atomic
            func.accept(v);
            return v;
        });
    }
}
