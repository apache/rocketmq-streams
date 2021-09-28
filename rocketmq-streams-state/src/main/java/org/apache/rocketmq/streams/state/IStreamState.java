package org.apache.rocketmq.streams.state;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @param <K>
 * @param <V>
 * @author arthur.liang
 */
public interface IStreamState<K, V> {

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this map contains no mapping for the
     * key.
     *
     * @param key
     * @return value
     */
    V get(K key);

    /**
     * Returns all values to which all specified keys is mapped
     *
     * @param key
     * @return
     */
    Map<K, V> getAll(List<K> key);

    /**
     * Associates the specified value with the specified key in this map (optional operation).  If the map previously
     * contained a mapping for the key, the old value is replaced by the specified value.
     *
     * @param key
     * @param value
     * @return
     */
    V put(K key, V value);

    /**
     * Associates the specified value with the specified key in this map (optional operation).  If the map previously
     * contained a mapping for the key, the old value will not be replaced by the specified value.
     *
     * @param key
     * @param value
     * @return
     */
    V putIfAbsent(K key, V value);

    /**
     * Removes the mapping for a key from this map if it is present (optional operation).
     *
     * @param key
     * @return
     */
    V remove(K key);

    /**
     * Removes the mapping for all keys from this map if it is present (optional operation).
     *
     * @param keys
     */
    void removeAll(List<K> keys);

    // Bulk Operations

    /**
     * Copies all of the mappings from the specified map to this map (optional operation).
     *
     * @param map
     */
    void putAll(Map<? extends K, ? extends V> map);

    /**
     * Removes all of the mappings from this map (optional operation). The map will be empty after this call returns.
     */
    void clear();

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     *
     * @return a set view of the keys contained in this map
     */
    Iterator<K> keyIterator();

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     *
     * @return a set view of the mappings contained in this map
     */
    Iterator<Map.Entry<K, V>> entryIterator();

    /**
     * Returns a {@link Set} view of the specified prefix mappings contained in this map.
     *
     * @param prefix
     * @returna set view of the mappings contained in this map
     */
    Iterator<Map.Entry<K, V>> entryIterator(String prefix);

}
