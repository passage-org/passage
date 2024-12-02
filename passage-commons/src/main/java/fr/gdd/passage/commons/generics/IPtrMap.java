package fr.gdd.passage.commons.generics;

public interface IPtrMap<K,V> {

    IPtrMap<K, V> put (K key, V val);
    IPtrMap<K, V> remove (K key);
    V get (K key);

}
