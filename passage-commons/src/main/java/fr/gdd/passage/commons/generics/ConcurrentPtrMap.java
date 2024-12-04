package fr.gdd.passage.commons.generics;

import com.google.common.collect.ConcurrentHashMultiset;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.atlas.iterator.Iter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Hash on keys then compare pointers of keys. It's meant to increase search
 * speed using hash, then retrieve the exact value using pointers.
 *  * @param <K> Key type.
 *  * @param <V> Value type.
 */
public class ConcurrentPtrMap<K, V> implements IPtrMap<K,V> {

    final ConcurrentHashMap<K, ConcurrentHashMultiset<ImmutablePair<K, V>>> map = new ConcurrentHashMap<>();
    int size = 0;

    public ConcurrentPtrMap<K, V> put (K key, V val) {
        map.putIfAbsent(key, ConcurrentHashMultiset.create());

        ConcurrentHashMultiset<ImmutablePair<K, V>> values = map.get(key);
        if (values.stream().filter(p -> p.getLeft() == key).toList().isEmpty()) {
            values.add(new ImmutablePair<>(key, val));
            ++size;
        } else {
            map.put(key, ConcurrentHashMultiset.create(
                    values.stream()
                            .map(p -> p.getLeft() == key ? new ImmutablePair<>(key, val) : p)
                            .collect(Collectors.toList())));
        }

        return this;
    }

    public ConcurrentPtrMap<K, V> remove (K key) {
        if (map.containsKey(key)) {
            ConcurrentHashMultiset<ImmutablePair<K, V>> values = map.get(key);
            values.removeIf(p -> p.getLeft() == key);
            --size;
            if (values.isEmpty()) {
                map.remove(key);
            }
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

    public V putIfAbsent(K key, V val) {
        var newSet = ConcurrentHashMultiset.create(Arrays.asList(ImmutablePair.of(key, val)));
        var nullIfAbs = map.putIfAbsent(key, newSet);
        if (Objects.nonNull(nullIfAbs)) {
            nullIfAbs.add(ImmutablePair.of(key, val));
        }
        return val;
    }
}
