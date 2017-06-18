package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.ForwardIterator;
import com.groupon.lex.metrics.lib.LazyMap;
import gnu.trove.TLongCollection;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import static java.lang.Long.min;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public abstract class ChainingTSCPair implements TimeSeriesCollectionPair {
    private static final Logger LOG = Logger.getLogger(ChainingTSCPair.class.getName());
    @NonNull
    private final CollectHistory history;
    private final TimestampChain timestamps;
    private final Map<GroupName, TsvChain> data = new THashMap<>();

    public ChainingTSCPair(@NonNull CollectHistory history, @NonNull ExpressionLookBack lookback) {
        this.history = history;

        Stream<TimeSeriesCollection> filtered;
        try {
            filtered = lookback.filter(new ForwardIterator<>(history.streamReversed().iterator()));
        } catch (UnsupportedOperationException ex) {
            LOG.log(Level.WARNING, "history reverse streaming not supported, fallback to duration hint");
            final DateTime end = history.getEnd();
            final DateTime begin = end.minus(lookback.hintDuration());
            filtered = history.stream(begin, end);
        }

        this.timestamps = new TimestampChain(filtered
                .map(TimeSeriesCollection::getTimestamp)
                .mapToLong(DateTime::getMillis)
                .collect(TLongHashSet::new, TLongHashSet::add, TLongHashSet::addAll));
        LOG.log(Level.INFO, "recovered {0} scrapes from history", timestamps.size());

        validatePrevious();  // Should never trigger.
    }

    @Override
    public String toString() {
        return "ChainingTSCPair[" + timestamps.toString() + "]";
    }

    @Override
    public abstract TimeSeriesCollection getCurrentCollection();

    @Override
    public Optional<TimeSeriesCollection> getPreviousCollection(int n) {
        if (n == 0)
            return Optional.of(getCurrentCollection());
        if (n < 0)
            throw new IllegalArgumentException("cannot look into the future");
        if (n - 1 >= timestamps.size()) return Optional.empty();
        return Optional.of(new TSCollectionImpl(timestamps.get(n - 1)));
    }

    @Override
    public int size() {
        return timestamps.size() + 1;
    }

    private void update_(TimeSeriesCollection tsc) {
        timestamps.add(tsc.getTimestamp());
        tsc.getTSValues().stream()
                .forEach(tsv -> {
                    data.compute(tsv.getGroup(), (grp, tsvChain) -> {
                        if (tsvChain == null)
                            tsvChain = new TsvChain(tsc.getTimestamp(), tsv);
                        else
                            tsvChain.add(tsc.getTimestamp(), tsv);
                        return tsvChain;
                    });
                });
    }

    private void apply_lookback_(ExpressionLookBack lookBack) {
        final TLongHashSet retainTs = lookBack.filter(new ForwardIterator<>(timestamps.streamReverse().mapToObj(TSCollectionImpl::new).iterator()))
                .map(TimeSeriesCollection::getTimestamp)
                .mapToLong(DateTime::getMillis)
                .collect(TLongHashSet::new, TLongHashSet::add, TLongHashSet::addAll);

        timestamps.retainAll(retainTs);
        data.values().forEach(tsvChain -> tsvChain.retainAll(retainTs));
    }

    private void validatePrevious() {
        try {
            if (!timestamps.isEmpty() && !(timestamps.front().isBefore(getCurrentCollection().getTimestamp())))
                throw new IllegalArgumentException("previous timestamps must be before current and ordered in reverse chronological order");
        } catch (IllegalArgumentException ex) {
            LOG.log(Level.SEVERE, "programmer error", ex);
            throw ex;
        }
    }

    protected final void update(TimeSeriesCollection tsc, ExpressionLookBack lookback, Runnable doBeforeValidation) {
        update_(tsc);
        apply_lookback_(lookback);
        doBeforeValidation.run();

        validatePrevious();
    }

    private static class TimestampChain {
        /**
         * All timestamps that are to be kept, sorted in ascending order,
         * placing the most recent collection last.
         */
        private final TLongList timestamps;

        public TimestampChain(@NonNull TLongSet set) {
            timestamps = new TLongArrayList(set);
            timestamps.sort();
        }

        public void add(@NonNull DateTime ts) {
            final long tsMillis = ts.getMillis();
            if (timestamps.isEmpty() || tsMillis > frontLong()) {
                timestamps.add(tsMillis);
            } else {
                final int bsPos = timestamps.binarySearch(tsMillis);
                if (bsPos < 0) { // Insert only if not present.
                    final int insPos = -(bsPos + 1);
                    timestamps.insert(insPos, tsMillis);
                }
            }
        }

        public boolean isEmpty() {
            return timestamps.isEmpty();
        }

        public int size() {
            return timestamps.size();
        }

        public boolean contains(long v) {
            return timestamps.binarySearch(v) >= 0;
        }

        public long get(int idx) {
            return timestamps.get(timestamps.size() - 1 - idx);
        }

        public LongStream streamReverse() {
            return LongStream.of(timestamps.toArray());
        }

        public DateTime front() {
            return new DateTime(frontLong(), DateTimeZone.UTC);
        }

        public long frontLong() {
            return get(0);
        }

        public DateTime back() {
            return new DateTime(backLong(), DateTimeZone.UTC);
        }

        public long backLong() {
            return get(size() - 1);
        }

        public void retainAll(TLongCollection values) {
            timestamps.retainAll(values);
        }

        @Override
        public String toString() {
            return timestamps.toString();
        }
    }

    private class TsvChain {
        private SoftReference<TLongObjectMap<TimeSeriesValue>> tailRef;

        public TsvChain() {
            tailRef = new SoftReference<>(null);
        }

        public TsvChain(@NonNull DateTime initTs, @NonNull TimeSeriesValue initTsv) {
            final TLongObjectHashMap<TimeSeriesValue> tail = new TLongObjectHashMap<>();
            tail.put(initTs.getMillis(), initTsv);
            tailRef = new SoftReference<>(tail);
        }

        public synchronized void add(@NonNull DateTime ts, @NonNull TimeSeriesValue tv) {
            final TLongObjectMap<TimeSeriesValue> tail = tailRef.get();
            if (tail != null)
                tail.put(ts.getMillis(), tv);
        }

        public synchronized Optional<TimeSeriesValue> get(GroupName name, long ts) {
            {
                final TLongObjectMap<TimeSeriesValue> tail = tailRef.get();
                /*
                 * Use the tail values immediately, if any of the following is true:
                 * - The timestamp ought to be included due to timestamps retention.
                 * - The timestamp is present in tail.
                 * - The tail contains at least one timestamp before/at the sought timestamp.
                 */
                if (tail != null && (timestamps.backLong() <= ts || tail.containsKey(ts) || !tail.keySet().forEach(v -> v > ts)))
                    return Optional.ofNullable(tail.get(ts));
            }

            final DateTime streamStart;
            if (timestamps.isEmpty())
                streamStart = new DateTime(ts, DateTimeZone.UTC);
            else
                streamStart = new DateTime(min(timestamps.backLong(), ts), DateTimeZone.UTC);

            final TLongObjectHashMap<TimeSeriesValue> tail = new TLongObjectHashMap<>();
            history.streamGroup(streamStart, name)
                    .forEach((tsvEntry) -> tail.put(tsvEntry.getKey().getMillis(), tsvEntry.getValue()));
            tailRef = new SoftReference<>(tail);

            return Optional.ofNullable(tail.get(ts));
        }

        public synchronized void retainAll(TLongCollection timestamps) {
            final TLongObjectMap<TimeSeriesValue> tail = tailRef.get();
            if (tail != null)
                tail.retainEntries((ts, value) -> timestamps.contains(ts));
        }
    }

    @RequiredArgsConstructor
    private class TSCollectionImpl extends AbstractTimeSeriesCollection {
        private final long ts;
        private final Map<GroupName, Optional<TimeSeriesValue>> tsvSet = new LazyMap<>(this::faultGroup, data.keySet());

        @Override
        public DateTime getTimestamp() {
            return new DateTime(ts, DateTimeZone.UTC);
        }

        @Override
        public boolean isEmpty() {
            return getGroups().isEmpty();
        }

        @Override
        public Set<GroupName> getGroups() {
            return tsvSet.entrySet().stream()
                    .filter(entry -> entry.getValue().isPresent())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        @Override
        public Set<SimpleGroupPath> getGroupPaths() {
            return tsvSet.entrySet().stream()
                    .collect(Collectors.groupingBy(entry -> entry.getKey().getPath())).entrySet().stream()
                    .filter(listing -> listing.getValue().stream().map(Map.Entry::getValue).anyMatch(Optional::isPresent))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }

        @Override
        public Collection<TimeSeriesValue> getTSValues() {
            return tsvSet.values().stream()
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
        }

        @Override
        public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
            return new TimeSeriesValueSet(tsvSet.entrySet().stream()
                    .filter(entry -> Objects.equals(entry.getKey().getPath(), name))
                    .map(Map.Entry::getValue)
                    .filter(Optional::isPresent)
                    .map(Optional::get));
        }

        @Override
        public Optional<TimeSeriesValue> get(GroupName name) {
            return tsvSet.getOrDefault(name, Optional.empty());
        }

        private Optional<TimeSeriesValue> faultGroup(GroupName name) {
            final TsvChain tsvChain = data.get(name);
            if (tsvChain == null)
                return Optional.empty();
            return tsvChain.get(name, ts);
        }
    }
}
