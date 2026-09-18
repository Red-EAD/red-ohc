package com.red.ohc.cache;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.red.ohc.index.Entry;

/** Live heap-snapshot views over the cache's native-backed CHM authority. */
final class MapViews<K, V> {
  private final OffHeapCache<K, V> cache;
  private final Set<K> keySet = new KeySetView();
  private final Collection<V> values = new ValuesView();
  private final Set<Map.Entry<K, V>> entrySet = new EntrySetView();

  MapViews(OffHeapCache<K, V> cache) {
    this.cache = Objects.requireNonNull(cache, "cache");
  }

  Set<K> keySet() {
    return keySet;
  }

  Set<K> mappedKeySet(V mappedValue) {
    return new MappedKeySetView(mappedValue);
  }

  Collection<V> values() {
    return values;
  }

  Set<Map.Entry<K, V>> entrySet() {
    return entrySet;
  }

  boolean containsValue(Object value) {
    return cache.containsValueForView(value);
  }

  private abstract class SnapshotIterator<T> implements Iterator<T> {
    private final Iterator<
            Map.Entry<
                Entry, Entry>>
        delegate = cache.data.entrySet().iterator();
    private Map.Entry<K, V> next;
    private Map.Entry<K, V> last;
    private boolean prepared;

    @Override
    public boolean hasNext() {
      prepare();
      return next != null;
    }

    @Override
    public T next() {
      prepare();
      if (next == null) {
        throw new java.util.NoSuchElementException();
      }
      last = next;
      next = null;
      prepared = false;
      return project(last);
    }

    @Override
    public void remove() {
      Map.Entry<K, V> snapshot = last;
      if (snapshot == null) {
        throw new IllegalStateException();
      }
      cache.removeIfPresent(snapshot.getKey());
      last = null;
    }

    private void prepare() {
      if (prepared) {
        return;
      }
      next = cache.nextViewEntry(delegate);
      prepared = true;
    }

    abstract T project(Map.Entry<K, V> snapshot);
  }

  private final class KeyIterator extends SnapshotIterator<K> {
    @Override
    K project(Map.Entry<K, V> snapshot) {
      return snapshot.getKey();
    }
  }

  private final class ValueIterator extends SnapshotIterator<V> {
    @Override
    V project(Map.Entry<K, V> snapshot) {
      return snapshot.getValue();
    }
  }

  private final class EntryIterator extends SnapshotIterator<Map.Entry<K, V>> {
    @Override
    Map.Entry<K, V> project(Map.Entry<K, V> snapshot) {
      return new CacheEntry(snapshot.getKey(), snapshot.getValue());
    }
  }

  private final class KeySetView extends AbstractSet<K> {
    @Override
    public int size() {
      return cache.size();
    }

    @Override
    public boolean isEmpty() {
      return cache.isEmpty();
    }

    @Override
    public boolean contains(Object key) {
      return cache.containsKey(key);
    }

    @Override
    public boolean remove(Object key) {
      @SuppressWarnings("unchecked")
      K typedKey = (K) key;
      return cache.removeIfPresent(typedKey);
    }

    @Override
    public void clear() {
      cache.clear();
    }

    @Override
    public Iterator<K> iterator() {
      return new KeyIterator();
    }

    @Override
    public boolean add(K key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends K> keys) {
      throw new UnsupportedOperationException();
    }
  }

  private final class MappedKeySetView extends AbstractSet<K> {
    private final V mappedValue;

    private MappedKeySetView(V mappedValue) {
      this.mappedValue = Objects.requireNonNull(mappedValue, "mappedValue");
    }

    @Override
    public int size() {
      return cache.size();
    }

    @Override
    public boolean isEmpty() {
      return cache.isEmpty();
    }

    @Override
    public boolean contains(Object key) {
      return cache.containsKey(key);
    }

    @Override
    public boolean add(K key) {
      return cache.putIfAbsentWithoutPrevious(key, mappedValue);
    }

    @Override
    public boolean addAll(Collection<? extends K> keys) {
      boolean changed = false;
      for (K key : keys) {
        changed |= add(key);
      }
      return changed;
    }

    @Override
    public boolean remove(Object key) {
      @SuppressWarnings("unchecked")
      K typedKey = (K) key;
      return cache.removeIfPresent(typedKey);
    }

    @Override
    public void clear() {
      cache.clear();
    }

    @Override
    public Iterator<K> iterator() {
      return new KeyIterator();
    }
  }

  private final class ValuesView extends AbstractCollection<V> {
    @Override
    public int size() {
      return cache.size();
    }

    @Override
    public boolean isEmpty() {
      return cache.isEmpty();
    }

    @Override
    public boolean contains(Object value) {
      return containsValue(value);
    }

    @Override
    public boolean remove(Object value) {
      if (value == null) {
        return false;
      }
      Iterator<Map.Entry<K, V>> iterator = new EntryIterator();
      while (iterator.hasNext()) {
        Map.Entry<K, V> snapshot = iterator.next();
        if (Objects.equals(value, snapshot.getValue())
            && cache.remove(snapshot.getKey(), snapshot.getValue())) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void clear() {
      cache.clear();
    }

    @Override
    public Iterator<V> iterator() {
      return new ValueIterator();
    }

    @Override
    public boolean add(V value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends V> values) {
      throw new UnsupportedOperationException();
    }
  }

  private final class EntrySetView extends AbstractSet<Map.Entry<K, V>> {
    @Override
    public int size() {
      return cache.size();
    }

    @Override
    public boolean isEmpty() {
      return cache.isEmpty();
    }

    @Override
    public boolean contains(Object object) {
      if (!(object instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<?, ?> candidate = (Map.Entry<?, ?>) object;
      Object key = candidate.getKey();
      Object value = candidate.getValue();
      if (key == null || value == null) {
        return false;
      }
      V current = cache.get(key);
      return current != null && Objects.equals(current, value);
    }

    @Override
    public boolean remove(Object object) {
      if (!(object instanceof Map.Entry)) {
        return false;
      }
      Map.Entry<?, ?> candidate = (Map.Entry<?, ?>) object;
      if (candidate.getKey() == null || candidate.getValue() == null) {
        return false;
      }
      return cache.remove(candidate.getKey(), candidate.getValue());
    }

    @Override
    public void clear() {
      cache.clear();
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new EntryIterator();
    }

    @Override
    public boolean add(Map.Entry<K, V> entry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Map.Entry<K, V>> entries) {
      throw new UnsupportedOperationException();
    }
  }

  private final class CacheEntry extends AbstractMap.SimpleEntry<K, V> {
    private CacheEntry(K key, V value) {
      super(key, value);
    }

    @Override
    public V setValue(V value) {
      Objects.requireNonNull(value, "value");
      V oldValue = getValue();
      if (!cache.replace(getKey(), oldValue, value)) {
        throw new ConcurrentModificationException();
      }
      super.setValue(value);
      return oldValue;
    }
  }
}
