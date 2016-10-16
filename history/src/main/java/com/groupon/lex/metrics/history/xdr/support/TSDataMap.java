package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.history.TSData;
import static java.lang.Thread.MIN_PRIORITY;
import java.lang.management.ManagementFactory;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.unmodifiableList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 *
 * @author ariane
 */
public class TSDataMap<K> implements Map<K, TSData> {
    public final static long DEFAULT_MAX_MMAP;
    public final static long DEFAULT_MAX_FD = 128;

    static {
        long max_mmap;
        try {
            // Try to figure out the total RAM in the system and assign half of it as MMAP limit.
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            Object attribute = mBeanServer.getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize");
            max_mmap = ((Number) attribute).longValue() / 2;
        } catch (Exception ex) {  // Also catch RuntimeExceptions, like ClassCastException.
            max_mmap = 16L * 1024 * 1024 * 1024;
            Logger.getLogger(TSDataMap.class.getName()).log(Level.SEVERE, "unable to figure out system RAM size", ex);
        }
        DEFAULT_MAX_MMAP = max_mmap;
    }

    private static interface EvictionQueueEntry<K> {
        public K getKey();

        public void updateTimestamp();

        public void markLost();

        public EvictionQueue getQueue();
    }

    private static class EvictionQueue<K> {
        private class EvictionQueueEntryImpl<K> implements EvictionQueueEntry<K> {
            final K key_;
            final long cost_;
            long remembered_timestamp_;
            final AtomicLong timestamp_;
            final AtomicBoolean lost_ = new AtomicBoolean(false);

            public EvictionQueueEntryImpl(K key, long cost) {
                key_ = requireNonNull(key);
                cost_ = cost;
                remembered_timestamp_ = System.nanoTime();
                timestamp_ = new AtomicLong(remembered_timestamp_);
            }

            @Override
            public K getKey() {
                return key_;
            }

            @Override
            public void updateTimestamp() {
                timestamp_.set(System.nanoTime());
            }

            @Override
            public void markLost() {
                if (lost_.getAndSet(true))
                    return;
                getQueue().total_cost_ -= cost_;
            }

            @Override
            public EvictionQueue getQueue() {
                return EvictionQueue.this;
            }
        }

        private long max_cost_;
        private long total_cost_ = 0;
        private final Consumer<K> evictor_;
        private final BiFunction<K, TSData, Long> cost_fn_;
        private final PriorityQueue<EvictionQueueEntryImpl<K>> entries_ = new PriorityQueue<>(Comparator.comparing(eqe -> eqe.remembered_timestamp_));

        public EvictionQueue(Consumer<K> evictor, BiFunction<K, TSData, Long> cost_fn, long max_cost) {
            max_cost_ = max_cost;
            cost_fn_ = requireNonNull(cost_fn);
            evictor_ = requireNonNull(evictor);
        }

        public EvictionQueueEntry<K> add(K key, TSData tsd) {
            final long cost = cost_fn_.apply(key, tsd);
            final EvictionQueueEntryImpl<K> entry = new EvictionQueueEntryImpl<>(key, cost);
            if (cost > 0) {
                synchronized (this) {
                    entries_.add(entry);
                    total_cost_ += cost;
                }
            }
            return entry;
        }

        public Optional<EvictionQueueEntry<K>> maybeAdd(K key, TSData tsd) {
            final long cost = cost_fn_.apply(key, tsd);
            if (cost <= 0)
                return Optional.empty();

            final EvictionQueueEntryImpl<K> entry = new EvictionQueueEntryImpl<>(key, cost);
            synchronized (this) {
                entries_.add(entry);
                total_cost_ += cost;
            }
            return Optional.of(entry);
        }

        public synchronized long getMaxCost() {
            return max_cost_;
        }

        public synchronized void setMaxCost(long max_cost) {
            max_cost_ = max_cost;
        }

        public synchronized void maybe_evict_() {
            while (total_cost_ > max_cost_) {
                final EvictionQueueEntryImpl<K> head = entries_.poll();
                if (head == null)
                    return;  // Everything is evicted.
                if (head.lost_.get())
                    continue;  // Lingering element that is no longer in use.
                if (head.remembered_timestamp_ != head.timestamp_.get()) {
                    head.remembered_timestamp_ = head.timestamp_.get();
                    entries_.offer(head);
                } else {
                    head.markLost();
                    evictor_.accept(head.getKey());
                }
            }
        }
    }

    private static class EntryValue {
        private final List<EvictionQueueEntry<?>> evictors_;
        private final AtomicReference<Reference<TSData>> value_;
        private final int hash_code_;

        public EntryValue(TSData value, Collection<EvictionQueueEntry<?>> evictors) {
            hash_code_ = (value == null ? 0 : value.hashCode());
            value_ = new AtomicReference<>(new SoftReference<>(value));
            evictors_ = new ArrayList<>(evictors);
        }

        public Reference<TSData> getReference() {
            return value_.get();
        }

        public TSData getValue() {
            updateTimestamp();
            TSData value = value_.get().get();
            if (value == null)
                markLost();
            return value;
        }

        public void updateTimestamp() {
            evictors_.forEach(EvictionQueueEntry::updateTimestamp);
        }

        private void markLost() {
            evictors_.forEach(EvictionQueueEntry::markLost);
        }

        public void onEviction() {
            final TSData v = value_.get().get();
            markLost();
            value_.set(new WeakReference<>(v));
        }

        @Override
        public int hashCode() {
            return hash_code_;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null)
                return false;
            if (!(o instanceof EntryValue))
                return false;
            final EntryValue other = (EntryValue) o;
            return Objects.equals(value_.get(), other.value_.get());
        }
    }

    private static final Map<Reference<TSData>, EntryValue> reference_map_ = new ConcurrentHashMap<>();
    private static final ReferenceQueue reference_queue_ = new ReferenceQueue();
    private static final AtomicReference<Thread> cleaner_ = new AtomicReference<>();
    private final Map<K, EntryValue> data_ = new ConcurrentHashMap<>();
    private final Collection<EvictionQueue<K>> evictors_ = unmodifiableList(Arrays.asList(
            new EvictionQueue<>(this::evict_, this::mem_cost_, DEFAULT_MAX_MMAP),
            new EvictionQueue<>(this::evict_, this::fd_cost_, DEFAULT_MAX_FD)));
    private final Function<? super K, ? extends TSData> resupplier_;

    private static void ensure_cleaner_() {
        if (cleaner_.get() != null)
            return;
        final Thread th = new Thread(() -> {
            for (;;) {
                try {
                    final Reference ref = reference_queue_.remove();
                    final EntryValue ev = reference_map_.remove(ref);
                    if (ev != null)
                        ev.markLost();
                } catch (InterruptedException ex) {
                    Logger.getLogger(TSDataMap.class.getName()).log(Level.SEVERE, "ignoring interruption", ex);
                }
            }
        });
        th.setDaemon(true);
        th.setName("TSDataMap-cleaner");
        th.setPriority(MIN_PRIORITY);
        if (cleaner_.compareAndSet(null, th))
            th.start();
    }

    public TSDataMap(Function<? super K, ? extends TSData> resupplier) {
        ensure_cleaner_();
        resupplier_ = requireNonNull(resupplier);
    }

    @Override
    public Set<Map.Entry<K, TSData>> entrySet() {
        return new AbstractSet<Map.Entry<K, TSData>>() {
            @Override
            public Iterator<Map.Entry<K, TSData>> iterator() {
                final Iterator<Map.Entry<K, EntryValue>> data_iter = data_.entrySet().iterator();
                return new Iterator<Entry<K, TSData>>() {
                    @Override
                    public boolean hasNext() {
                        return data_iter.hasNext();
                    }

                    @Override
                    public Map.Entry<K, TSData> next() {
                        final Map.Entry<K, EntryValue> n = data_iter.next();
                        return new Map.Entry<K, TSData>() {
                            private TSData remembered_value_ = null;

                            @Override
                            public K getKey() {
                                return n.getKey();
                            }

                            @Override
                            public TSData getValue() {
                                if (remembered_value_ == null) {
                                    remembered_value_ = n.getValue().getValue();
                                    if (remembered_value_ == null)
                                        remembered_value_ = get(n.getKey());
                                }
                                return remembered_value_;
                            }

                            @Override
                            public TSData setValue(TSData tsd) {
                                throw new UnsupportedOperationException("TSDataMap entrySet is immutable, use put instead");
                            }

                            @Override
                            public int hashCode() {
                                return n.getKey().hashCode() ^ n.getValue().hashCode();
                            }

                            @Override
                            public boolean equals(Object o) {
                                if (o == null)
                                    return false;
                                if (!(o instanceof Map.Entry))
                                    return false;
                                Map.Entry<?, ?> other = (Map.Entry<?, ?>) o;
                                return Objects.equals(getKey(), other.getKey()) && Objects.equals(getValue(), other.getValue());
                            }
                        };
                    }
                };
            }

            @Override
            public int size() {
                return data_.size();
            }
        };
    }

    @Override
    public Set<K> keySet() {
        return data_.keySet();
    }

    @Override
    public Collection<TSData> values() {
        return new AbstractCollection<TSData>() {
            @Override
            public Iterator<TSData> iterator() {
                final Iterator<Entry<K, EntryValue>> data_iter = data_.entrySet().iterator();
                return new Iterator<TSData>() {
                    @Override
                    public boolean hasNext() {
                        return data_iter.hasNext();
                    }

                    @Override
                    public TSData next() {
                        final Entry<K, EntryValue> n = data_iter.next();
                        TSData value = n.getValue().getValue();
                        if (value != null)
                            return value;
                        return get(n.getKey());
                    }
                };
            }

            @Override
            public int size() {
                return data_.size();
            }
        };
    }

    @Override
    public void clear() {
        data_.clear();  // XXX expire all values?
    }

    public TSData put(K key, TSData tsd) {
        final EntryValue ev = new_entry_value_(key, tsd);
        return Optional.ofNullable(data_.put(key, ev))
                .map(old_ev -> {
                    old_ev.markLost();
                    return old_ev.getValue();
                })
                .orElse(null);
    }

    @Override
    public void putAll(Map<? extends K, ? extends TSData> m) {
        data_.putAll(m.entrySet().stream().collect(Collectors.toMap(Entry::getKey, entry -> new_entry_value_(entry.getKey(), entry.getValue()))));
    }

    @Override
    public TSData remove(Object key) {
        return Optional.ofNullable(data_.remove(key))
                .map(ev -> {
                    ev.markLost();
                    return ev.getValue();
                })
                .orElse(null);
    }

    @Override
    public boolean remove(Object key, Object value) {
        final SoftReference<Object> comparison = new SoftReference<>(value);
        final EntryValue old_val = data_.computeIfPresent((K) key, (K k, EntryValue d_val) -> {
            if (Objects.equals(((EntryValue) d_val).getReference(), comparison))
                return null;
            return d_val;
        });
        return old_val != null && Objects.equals(old_val.getReference(), comparison);
    }

    @Override
    public TSData get(Object key) {
        class Tmp {
            public TSData result = null;
        }
        final Tmp tmp = new Tmp();

        data_.computeIfPresent((K) key, (k, d_val) -> {
            tmp.result = d_val.getValue();
            if (tmp.result == null) {
                d_val.markLost();
                tmp.result = resupplier_.apply(k);
                d_val = new_entry_value_(k, tmp.result);
            }
            return d_val;
        });
        return tmp.result;
    }

    @Override
    public boolean containsKey(Object key) {
        return data_.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        final SoftReference<Object> comparison = new SoftReference<>(value);
        return data_.values().stream().anyMatch(v -> Objects.equals(v.getReference(), comparison));
    }

    @Override
    public boolean isEmpty() {
        return data_.isEmpty();
    }

    @Override
    public int size() {
        return data_.size();
    }

    @Override
    public int hashCode() {
        return entrySet().stream().mapToInt(Entry::hashCode).sum();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (!(o instanceof Map))
            return false;

        if (o instanceof TSDataMap) {
            final TSDataMap<?> other = (TSDataMap) o;
            return Objects.equals(data_, other.data_);
        } else {
            final Map<?, ?> other = (Map) o;
            if (other.size() != size())
                return false;
            return other.entrySet().stream()
                    .allMatch(o_entry -> {
                        final EntryValue my_value = data_.get((K) o_entry.getKey());
                        SoftReference<Object> comparison = new SoftReference<>(o_entry.getValue());
                        return Objects.equals(my_value.getReference(), comparison);
                    });
        }
    }

    private EntryValue new_entry_value_(K key, TSData tsd) {
        final List<EvictionQueueEntry<?>> evictor_entries;
        if (tsd == null) {
            evictor_entries = EMPTY_LIST;
        } else {
            evictor_entries = evictors_.stream()
                    .map(eq -> eq.maybeAdd(key, tsd))
                    .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                    .collect(Collectors.toList());
        }
        final EntryValue ev = new EntryValue(tsd, evictor_entries);
        if (tsd != null)
            reference_map_.put(new PhantomReference<>(tsd, reference_queue_), ev);
        return ev;
    }

    private void evict_(K key) {
        final EntryValue tsd = data_.get(key);
        if (tsd != null)
            tsd.onEviction();
    }

    @Deprecated
    private long mem_cost_(K key, TSData tsd) {
        return 0;
    }

    private long fd_cost_(K key, TSData tsd) {
        return (tsd.getFileChannel().isPresent() ? 1 : 0);
    }
}
