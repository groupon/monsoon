/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.singleton;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * A map that lazily computes its mapped values.
 */
public class LazyMap<K, V> implements Map<K, V> {
    private static final Object SENTINEL = new Object();
    private final Function<? super K, ? extends V> compute;
    private final Map<K, V> internalMap;

    public LazyMap(Function<? super K, ? extends V> compute) {
        this(compute, HashMap<K, V>::new);
    }

    public LazyMap(Function<? super K, ? extends V> compute, Supplier<Map<K, V>> mapSupplier) {
        this.compute = compute;
        this.internalMap = mapSupplier.get();
    }

    public LazyMap(Function<? super K, ? extends V> compute, Collection<? extends K> initialKeys) {
        this(compute, initialKeys, HashMap<K, V>::new);
    }

    public LazyMap(Function<? super K, ? extends V> compute, Collection<? extends K> initialKeys, Supplier<Map<K, V>> mapSupplier) {
        this(compute, mapSupplier);
        initialKeys.forEach(k -> internalMap.put(k, getSentinel()));
    }

    @Override
    public boolean isEmpty() { return internalMap.isEmpty(); }
    @Override
    public int size() { return internalMap.size(); }

    @Override
    public V get(Object key) {
        V v = internalMap.get(key);
        if (v == SENTINEL) {
            v = compute.apply((K)key);
            internalMap.put((K)key, v);
        }
        return v;
    }

    @Override
    public V put(K key, V value) {
        V v = internalMap.put(key, value);
        if (v == SENTINEL)
            v = compute.apply(key);
        return v;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        internalMap.putAll(m);
    }

    @Override
    public V remove(Object k) {
        V v = internalMap.remove(k);
        if (v == SENTINEL)
            v = compute.apply((K)k);
        return v;
    }

    @Override
    public Set<K> keySet() {
        return internalMap.keySet();
    }

    @Override
    public Collection<V> values() {
        return new ValuesCollection();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    @Override
    public boolean containsKey(Object key) {
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        List<Map.Entry<K, V>> sentinelValues = new ArrayList<>();

        for (Map.Entry<K, V> e : entrySet()) {
            if (internalMap.get(e.getKey()) == SENTINEL)
                sentinelValues.add(e);
            else if (Objects.equals(e.getValue(), value))
                return true;
        }

        for (Map.Entry<K, V> e : sentinelValues) {
            if (Objects.equals(e.getValue(), value))
                return true;
        }

        return false;
    }

    @Override
    public void clear() {
        internalMap.clear();
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof Map)) return false;
        Map m = (Map)o;

        return entrySet().equals(m.entrySet());
    }

    @SuppressWarnings("unchecked")
    private static <T> T getSentinel() {
        return (T)SENTINEL;
    }

    @RequiredArgsConstructor
    private class EntrySetIterator implements Iterator<Map.Entry<K, V>> {
        private final Iterator<K> internalIter;

        @Override
        public boolean hasNext() { return internalIter.hasNext(); }
        @Override
        public Map.Entry<K, V> next() { return new LazyMapEntry(internalIter.next()); }
        @Override
        public void remove() { internalIter.remove(); }
    }

    @RequiredArgsConstructor
    @ToString
    @Getter
    private class LazyMapEntry implements Map.Entry<K, V> {
        private final K key;

        @Override
        public V getValue() { return LazyMap.this.get(key); }
        @Override
        public V setValue(V v) { return LazyMap.this.put(key, v); }

        @Override
        public int hashCode() {
            return (getKey() == null   ? 0 : getKey().hashCode()) ^
                    (getValue() == null ? 0 : getValue().hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) return false;
            if (!(o instanceof Map.Entry)) return false;

            final Map.Entry<?, ?> other = (Map.Entry<?, ?>)o;
            return Objects.equals(getKey(), other.getKey()) && Objects.equals(getValue(), other.getValue());
        }
    }

    private class EntrySet implements Set<Map.Entry<K, V>> {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntrySetIterator(internalMap.keySet().iterator());
        }

        @Override
        public boolean addAll(Collection<? extends Map.Entry<K, V>> c) {
            boolean changed = false;

            for (Map.Entry<K, V> e : c) {
                K k = e.getKey();

                V v = LazyMap.this.get(k);
                final boolean present = !(v == null && !internalMap.containsKey(k));
                if (present && Objects.equals(e.getValue(), v))
                    continue;

                internalMap.put(k, e.getValue());
                changed = true;
            }
            return changed;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            for (Object eObj : c) {
                if (eObj == null || !(eObj instanceof Map.Entry)) return false;
                Map.Entry e = (Map.Entry)eObj;
                Object k = e.getKey();

                V v = LazyMap.this.get((K)k);
                if (v == null && !internalMap.containsKey((K)k))
                    return false;
                if (!Objects.equals(e.getValue(), v))
                    return false;
            }

            return true;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            boolean changed = false;

            for (Object eObj : c) {
                if (eObj == null || !(eObj instanceof Map.Entry)) continue;
                Map.Entry e = (Map.Entry)eObj;
                Object k = e.getKey();

                V v = LazyMap.this.get(k);
                if (Objects.equals(e.getValue(), v)) {
                    if (LazyMap.this.remove(k, v))
                        changed = true;
                }
            }
            return changed;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            boolean changed = false;

            for (Object eObj : c) {
                if (eObj == null || !(eObj instanceof Map.Entry)) continue;
                Map.Entry e = (Map.Entry)eObj;
                Object k = e.getKey();

                V v = LazyMap.this.get(k);
                if (!Objects.equals(e.getValue(), v)) {
                    if (LazyMap.this.remove(k, v))
                        changed = true;
                }
            }
            return changed;
        }

        @Override
        public boolean add(Map.Entry<K, V> e) { return addAll(singleton(e)); }
        @Override
        public boolean contains(Object e) { return containsAll(singleton(e)); }
        @Override
        public boolean remove(Object o) { return removeAll(singleton(o)); }
        @Override
        public void clear() { LazyMap.this.clear(); }
        @Override
        public int size() { return LazyMap.this.size(); }
        @Override
        public boolean isEmpty() { return LazyMap.this.isEmpty(); }

        @Override
        public Object[] toArray() {
            final Object[] result = new Object[size()];
            Iterator<Entry<K, V>> iter = iterator();
            for (int idx = 0; iter.hasNext(); ++idx) {
                result[idx] = iter.next();
            }
            return result;
        }

        @Override
        public <T> T[] toArray(T[] a) {
            if (a.length < size())
                a = Arrays.copyOf(a, size());

            Iterator<Entry<K, V>> iter = iterator();
            for (int idx = 0; iter.hasNext(); ++idx) {
                a[idx] = (T)iter.next();
            }
            return a;
        }

        @Override
        public int hashCode() {
            int sumOfHashcodes = 0;
            final Iterator<Entry<K, V>> i = iterator();
            while (i.hasNext()) {
                sumOfHashcodes += i.next().hashCode();
            }
            return sumOfHashcodes;
        }

        @Override
        public boolean equals(Object otherObj) {
            if (otherObj == null) return false;
            if (!(otherObj instanceof Set)) return false;

            final List<Map.Entry<K, V>> sentinelValues = new ArrayList<>();
            final Set other = (Set)otherObj;

            // Compare size.
            if (size() != other.size()) return false;

            // Compare all non-sentinel values.
            final Iterator<Entry<K, V>> iter = iterator();
            while (iter.hasNext()) {
                final Entry<K, V> myElem = iter.next();
                if (internalMap.get(myElem.getKey()) == SENTINEL)
                    sentinelValues.add(myElem);  // Process sentinels last.
                else if (!other.contains(myElem))
                    return false;
            }

            // Now process sentinels.
            for (Map.Entry<K, V> myElem : sentinelValues)
                if (!other.contains(myElem)) return false;

            return true;
        }
    }

    private class ValuesCollection implements Collection<V> {
        @Override
        public int size() { return LazyMap.this.size(); }
        @Override
        public boolean isEmpty() { return LazyMap.this.isEmpty(); }

        @Override
        public boolean contains(Object o) {
            return containsAll(singleton(o));
        }

        @Override
        public Iterator<V> iterator() {
            return new Iterator<V>() {
                private final Iterator<Map.Entry<K, V>> underlying = new EntrySetIterator(internalMap.keySet().iterator());

                @Override
                public boolean hasNext() { return underlying.hasNext(); }
                @Override
                public V next() { return underlying.next().getValue(); }
                @Override
                public void remove() { underlying.remove(); }
            };
        }

        @Override
        public Object[] toArray() {
            final Object[] result = new Object[size()];
            Iterator<V> iter = iterator();
            for (int idx = 0; iter.hasNext(); ++idx) {
                result[idx] = iter.next();
            }
            return result;
        }

        @Override
        public <T> T[] toArray(T[] a) {
            if (a.length < size())
                a = Arrays.copyOf(a, size());

            Iterator<V> iter = iterator();
            for (int idx = 0; iter.hasNext(); ++idx) {
                a[idx] = (T)iter.next();
            }
            return a;
        }

        @Override
        public boolean add(V e) {
            throw new UnsupportedOperationException("Can't add value without key.");
        }

        @Override
        public boolean remove(Object o) {
            return removeAll(singleton(o));
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            if (c.isEmpty()) return true;

            // Operate on a copy of c, so we can remove items from c instead.
            Collection<?> copy = new ArrayList<>(c);
            List<Map.Entry<K, V>> sentinalValues = new ArrayList<>();  // Delay sentinal resolver.

            // Remove all non-sentinel values from copy.
            final Iterator<Map.Entry<K, V>> iter = new EntrySetIterator(internalMap.keySet().iterator());
            while (iter.hasNext()) {
                Map.Entry<K, V> e = iter.next();

                if (internalMap.get(e.getKey()) == SENTINEL)
                    sentinalValues.add(e);
                else
                    copy.remove(e.getValue());

                if (copy.isEmpty()) return true;
            }

            // Remove all sentinel values from copy.
            for (Map.Entry<K, V> sentinel : sentinalValues) {
                copy.remove(sentinel.getValue());
                if (copy.isEmpty()) return true;
            }

            assert(!copy.isEmpty());
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException("Can't add values without keys.");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            boolean changed = false;

            Iterator<V> iter = iterator();
            while (iter.hasNext()) {
                V next = iter.next();

                if (c.contains(next)) {
                    changed = true;
                    iter.remove();
                }
            }
            return changed;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            boolean changed = false;

            Iterator<V> iter = iterator();
            while (iter.hasNext()) {
                V next = iter.next();

                if (!c.contains(next)) {
                    changed = true;
                    iter.remove();
                }
            }
            return changed;
        }

        @Override
        public void clear() {
            LazyMap.this.clear();
        }
    }
}
