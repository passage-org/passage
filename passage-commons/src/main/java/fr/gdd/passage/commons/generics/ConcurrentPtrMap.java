package fr.gdd.passage.commons.generics;

import com.google.common.collect.ConcurrentHashMultiset;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Hash on keys then compare pointers of keys. It's meant to increase search
 * speed using hash, then retrieve the exact value using pointers.
 *  * @param <F> Key type.
 *  * @param <T> Value type.
 */
public class ConcurrentPtrMap<F, T> {

    ConcurrentHashMap<F, ConcurrentHashMultiset<Pair<F, T>>> map = new ConcurrentHashMap<>();
    int size = 0;

    public ConcurrentPtrMap<F, T> put (F key, T val) {
        map.putIfAbsent(key, ConcurrentHashMultiset.create());

        ConcurrentHashMultiset<Pair<F, T>> values = map.get(key);
        if (values.stream().filter(p -> p.getLeft() == key).toList().isEmpty()) {
            values.add(new ImmutablePair<>(key, val));
            ++size;
        } else {
            map.put(key, ConcurrentHashMultiset.create(values.stream().map(p -> p.getLeft() == key ? new ImmutablePair<>(key, val) : p).toList()));
        }

        return this;
    }

    public ConcurrentPtrMap<F, T> remove (F key) {
        if (map.containsKey(key)) {
            ConcurrentHashMultiset<Pair<F, T>> values = map.get(key);
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
            ConcurrentHashMultiset<Pair<F, T>> values = map.get(key);
            List<Pair<F, T>> result = values.stream().filter(p -> p.getLeft() == key).toList();
            if (!result.isEmpty()) {
                return result.getFirst().getRight();
            }
        }
        return null;
    }
}
