package fr.gdd.passage.commons.generics;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Hash on keys then compare pointers of keys. It's meant to increase search
 * speed using hash, then retrieve the exact value using pointers.
 *  * @param <K> Key type.
 *  * @param <V> Value type.
 */
public class PtrMap<K, V> implements IPtrMap<K, V> {

    HashMap<K, ArrayList<Pair<K, V>>> map = new HashMap<>();
    int size = 0;

    public PtrMap<K, V> put (K key, V val) {
        if (!map.containsKey(key)) {
            map.put(key, new ArrayList<>());
        }

        ArrayList<Pair<K, V>> values = map.get(key);
        if (values.stream().filter(p -> p.getLeft() == key).toList().isEmpty()) {
            values.add(new ImmutablePair<>(key, val));
            ++size;
        } else {
            map.put(key, new ArrayList<>(values.stream().map(p -> p.getLeft() == key ? new ImmutablePair<>(key, val) : p).toList()));
        }

        return this;
    }

    public PtrMap<K, V> remove (K key) {
        if (map.containsKey(key)) {
            ArrayList<Pair<K, V>> values = map.get(key);
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
            ArrayList<Pair<K, V>> values = map.get(key);
            List<Pair<K, V>> result = values.stream().filter(p -> p.getLeft() == key).toList();
            if (!result.isEmpty()) {
                return result.getFirst().getRight();
            }
        }
        return null;
    }
}
