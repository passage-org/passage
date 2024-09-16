package fr.gdd.passage.commons.generics;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Hash on keys then compare pointers of keys. It's meant to increase search
 * speed using hash, then retrieve the exact value using pointers.
 *  * @param <F> Key type.
 *  * @param <T> Value type.
 */
public class PtrMap<F, T> {

    HashMap<F, ArrayList<Pair<F, T>>> map = new HashMap<>();
    int size = 0;

    public PtrMap<F, T> put (F key, T val) {
        if (!map.containsKey(key)) {
            map.put(key, new ArrayList<>());
        }

        ArrayList<Pair<F, T>> values = map.get(key);
        if (values.stream().filter(p -> p.getLeft() == key).toList().isEmpty()) {
            values.add(new ImmutablePair<>(key, val));
            ++size;
        } else {
            map.put(key, new ArrayList<>(values.stream().map(p -> p.getLeft() == key ? new ImmutablePair<>(key, val) : p).toList()));
        }

        return this;
    }

    public PtrMap<F, T> remove (F key) {
        if (map.containsKey(key)) {
            ArrayList<Pair<F, T>> values = map.get(key);
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

    public T get (F key) {
        if (map.containsKey(key)) {
            ArrayList<Pair<F, T>> values = map.get(key);
            List<Pair<F, T>> result = values.stream().filter(p -> p.getLeft() == key).toList();
            if (!result.isEmpty()) {
                return result.getFirst().getRight();
            }
        }
        return null;
    }
}
