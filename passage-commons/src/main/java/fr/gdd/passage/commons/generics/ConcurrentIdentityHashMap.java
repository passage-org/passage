package fr.gdd.passage.commons.generics;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentIdentityHashMap<K,V> {

    public final ConcurrentHashMap<KeyWrapper<K>, V> map = new ConcurrentHashMap<>();

    public static <K> KeyWrapper<K> buildKeyOf(K key) {
        return new KeyWrapper<>(key);
    }

    public void put(K key, V value) {
        map.put(new KeyWrapper<>(key), value);
    }

    public V get(K key) {
        return map.get(new KeyWrapper<>(key));
    }

    public V remove(K key) {
        return map.remove(new KeyWrapper<>(key));
    }

    /* ****************************************************************************** */

    /**
     * Wrapper allows hashing on identity instead of hashed value.
     */
    public static class KeyWrapper<K> {
        private final K key;

        public KeyWrapper(K key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof KeyWrapper && ((KeyWrapper<K>) obj).key == this.key;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(key);
        }
    }
}
