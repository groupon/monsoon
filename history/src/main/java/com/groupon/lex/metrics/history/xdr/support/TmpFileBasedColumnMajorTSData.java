package com.groupon.lex.metrics.history.xdr.support;

import com.google.common.collect.Iterators;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import com.groupon.lex.metrics.history.v2.xdr.dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.xdr.ColumnMajorTSData;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.synchronizedSet;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;
import org.acplt.oncrpc.XdrEncodingStream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TmpFileBasedColumnMajorTSData implements ColumnMajorTSData {
    private static final Compression TMP_FILE_COMPRESSION = Compression.NONE;
    private final TLongList timestamps;
    private final Map<GroupName, Group> groups;
    private final Map<GroupName, Set<DateTime>> timestampsByGroup;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TLongList timestamps = new TLongArrayList();
        private final Map<GroupName, GroupWriter> writers = new ConcurrentHashMap<>();
        private final Map<GroupName, Set<DateTime>> timestampsByGroup = new ConcurrentHashMap<>();

        private Builder() {
            /* SKIP */
        }

        public Builder with(TimeSeriesCollection tsc) throws IOException {
            return with(singleton(tsc));
        }

        public Builder with(Collection<? extends TimeSeriesCollection> tsdata) throws IOException {
            try {
                for (final TimeSeriesCollection tsc : tsdata) {
                    tsc.getTSValues().parallelStream()
                            .peek(tsv -> {
                                timestampsByGroup.computeIfAbsent(tsv.getGroup(), (g) -> synchronizedSet(new HashSet<>()))
                                        .add(tsc.getTimestamp());
                            })
                            .forEach(tsv -> {
                                final GroupWriter groupWriter = writers.computeIfAbsent(
                                        tsv.getGroup(),
                                        (g) -> new GroupWriter());

                                try {
                                    groupWriter.fixBacklog(timestamps.size());

                                    groupWriter.add(tsv.getMetrics());
                                } catch (IOException ex) {
                                    throw new RuntimeIOException(ex);
                                }
                            });

                    timestamps.add(tsc.getTimestamp().getMillis());
                }

                return this;
            } catch (RuntimeIOException ex) {
                throw ex.getEx();
            }
        }

        /**
         * Pad all groups with empty maps, to ensure it's the same size as
         * timestamps list.
         *
         * We want to keep all the Group instances to have the same number of
         * maps as the timestamps list, so we can zip the two together.
         */
        private void fixBacklog() throws IOException {
            try {
                writers.values().parallelStream()
                        .forEach(groupWriter -> {
                            try {
                                groupWriter.fixBacklog(timestamps.size());
                            } catch (IOException ex) {
                                throw new RuntimeIOException(ex);
                            }
                        });
            } catch (RuntimeIOException ex) {
                throw ex.getEx();
            }
        }

        public TmpFileBasedColumnMajorTSData build() throws IOException {
            fixBacklog();

            final Map<GroupName, Group> groups = writers.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, groupWriterEntry -> groupWriterEntry.getValue().asReader()));
            return new TmpFileBasedColumnMajorTSData(timestamps, groups, timestampsByGroup);
        }
    }

    @Override
    public Collection<DateTime> getTimestamps() {
        Collection<DateTime> result = new ArrayList<>(timestamps.size());
        timestamps.forEach(ts -> {
            result.add(new DateTime(ts, DateTimeZone.UTC));
            return true;
        });
        return result;
    }

    @Override
    public Set<GroupName> getGroupNames() {
        return unmodifiableSet(groups.keySet());
    }

    @Override
    public Collection<DateTime> getGroupTimestamps(GroupName group) {
        return unmodifiableSet(timestampsByGroup.getOrDefault(group, emptySet()));
    }

    @Override
    public Set<MetricName> getMetricNames(GroupName group) {
        final Group groupData = groups.get(group);
        if (groupData == null)
            return emptySet();
        return groupData.getMetricNames();
    }

    @Override
    public Map<DateTime, MetricValue> getMetricValues(GroupName group, MetricName metric) {
        final Group groupData = groups.get(group);
        if (groupData == null || !groupData.getMetricNames().contains(metric))
            return emptyMap();

        return new MetricValuesMap(timestamps, groupData, metric);
    }

    private static class MetricValuesMap extends AbstractMap<DateTime, MetricValue> {
        private final MetricValuesEntrySet entrySet;

        public MetricValuesMap(TLongList timestamps, Group groupData, MetricName metric) {
            entrySet = new MetricValuesEntrySet(timestamps, groupData, metric);
        }

        @Override
        public Set<Map.Entry<DateTime, MetricValue>> entrySet() {
            return entrySet;
        }

        @RequiredArgsConstructor
        private static class MetricValuesEntrySet extends AbstractSet<Map.Entry<DateTime, MetricValue>> {
            @NonNull
            private final TLongList timestamps;
            @NonNull
            private final Group groupData;
            @NonNull
            private final MetricName metric;

            @Override
            public Iterator<Map.Entry<DateTime, MetricValue>> iterator() {
                return Iterators.transform(
                        Iterators.filter(
                                groupData.iterator(timestamps, metric),
                                timestampedMetric -> timestampedMetric.getValue().isPresent()),
                        timestampedMetric -> SimpleMapEntry.create(timestampedMetric.getTimestamp(), timestampedMetric.getValue().get()));
            }

            @Override
            public int size() {
                return timestamps.size();
            }
        }
    }

    private static class GroupWriter {
        private final Map<MetricName, MetricWriter> metrics = new ConcurrentHashMap<>();

        public void add(Map<MetricName, MetricValue> tsv) throws IOException {
            try {
                tsv.entrySet().parallelStream()
                        .forEach(entry -> {
                            final MetricWriter f = metrics.computeIfAbsent(
                                    entry.getKey(),
                                    (m) -> {
                                        try {
                                            return new MetricWriter();
                                        } catch (IOException ex) {
                                            throw new RuntimeIOException(ex);
                                        }
                                    });

                            try {
                                f.add(entry.getValue());
                            } catch (IOException ex) {
                                throw new RuntimeIOException(ex);
                            }
                        });
            } catch (RuntimeIOException ex) {
                throw ex.getEx();
            }
        }

        public void fixBacklog(int padUntil) throws IOException {
            try {
                metrics.values().parallelStream()
                        .forEach(f -> {
                            try {
                                f.fixBacklog(padUntil);
                            } catch (IOException ex) {
                                throw new RuntimeIOException(ex);
                            }
                        });
            } catch (RuntimeIOException ex) {
                throw ex.getEx();
            }
        }

        public Group asReader() {
            return new Group(metrics.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().asReader())));
        }
    }

    @AllArgsConstructor
    private static class Group {
        private final Map<MetricName, Metric> metrics;

        public Set<MetricName> getMetricNames() {
            return metrics.keySet();
        }

        public Iterator<TimestampedMetric> iterator(TLongList timestamps, MetricName metricName) {
            final Metric m = metrics.get(metricName);
            if (m == null) return emptyIterator();
            return m.iterator(timestamps);
        }
    }

    private static class MetricWriter {
        private final GCCloseable<TmpFile<XdrAbleMetricEntry>> tmpFile;
        private final DictionaryForWrite dictionary = new DictionaryForWrite();

        public MetricWriter() throws IOException {
            this.tmpFile = new GCCloseable<>(new TmpFile<>(TMP_FILE_COMPRESSION));
        }

        public void add(@NonNull MetricValue metric) throws IOException {
            try {
                tmpFile.get().add(new XdrAbleMetricEntry(dictionary, Optional.of(metric)));
            } catch (OncRpcException ex) {
                throw new IOException(ex);
            }
        }

        public void fixBacklog(int padUntil) throws IOException {
            try {
                for (int i = tmpFile.get().size(); i < padUntil; ++i)
                    tmpFile.get().add(new XdrAbleMetricEntry(dictionary, Optional.empty()));
            } catch (OncRpcException ex) {
                throw new IOException(ex);
            }
        }

        public Metric asReader() {
            return new Metric(tmpFile);
        }
    }

    @RequiredArgsConstructor
    private static class Metric {
        private final GCCloseable<TmpFile<XdrAbleMetricEntry>> tmpFile;

        public Iterator<TimestampedMetric> iterator(TLongList timestamps) {
            final TLongIterator timestampIter = timestamps.iterator();
            return Iterators.transform(
                    iterator(),
                    metric -> new TimestampedMetric(new DateTime(timestampIter.next(), DateTimeZone.UTC), metric));
        }

        public Iterator<Optional<MetricValue>> iterator() {
            return new IteratorImpl(tmpFile);
        }

        private static class IteratorImpl implements Iterator<Optional<MetricValue>> {
            private final Iterator<XdrAbleMetricEntry> inner;
            private DictionaryDelta dictionary = new DictionaryDelta();

            /*
             * Bind the lifetime of tmpFile to the lifetime of this iterator.
             */
            private final GCCloseable<TmpFile<XdrAbleMetricEntry>> tmpFile;

            public IteratorImpl(@NonNull GCCloseable<TmpFile<XdrAbleMetricEntry>> tmpFile) {
                this.tmpFile = tmpFile;
                try {
                    inner = this.tmpFile.get().iterator(XdrAbleMetricEntry::new);
                } catch (IOException | OncRpcException ex) {
                    throw new DecodingException("cannot read: decoding failed", ex);
                }
            }

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public Optional<MetricValue> next() {
                return inner.next()
                        .decode(dictionary, (updatedDictionary) -> dictionary = updatedDictionary);
            }
        }
    }

    private static class XdrAbleMetricEntry implements XdrAble {
        private boolean present = false;
        private metric_value metric;
        private dictionary_delta dd;

        public XdrAbleMetricEntry() {
            /* SKIP */
        }

        public XdrAbleMetricEntry(@NonNull DictionaryForWrite dictionary, @NonNull Optional<MetricValue> value) {
            this.present = value.isPresent();
            if (value.isPresent()) {
                this.metric = ToXdr.metricValue(value.get(), dictionary.getStringTable()::getOrCreate);
                this.dd = dictionary.encode();
            }
            dictionary.reset();
        }

        public Optional<MetricValue> decode(DictionaryDelta inputDictionary, Consumer<DictionaryDelta> updateDictionary) {
            if (!present) return Optional.empty();

            final DictionaryDelta dictionary = new DictionaryDelta(dd, inputDictionary);
            final MetricValue result = FromXdr.metricValue(metric, dictionary::getString);

            updateDictionary.accept(dictionary);
            return Optional.of(result);
        }

        @Override
        public void xdrEncode(XdrEncodingStream stream) throws OncRpcException, IOException {
            stream.xdrEncodeBoolean(present);
            if (present) {
                dd.xdrEncode(stream);
                metric.xdrEncode(stream);
            }
        }

        @Override
        public void xdrDecode(XdrDecodingStream stream) throws OncRpcException, IOException {
            present = stream.xdrDecodeBoolean();
            if (!present) {
                metric = null;
                dd = null;
            } else {
                dd = new dictionary_delta(stream);
                metric = new metric_value(stream);
            }
        }
    }

    @Value
    private static class TimestampedMetric {
        @NonNull
        private final DateTime timestamp;
        @NonNull
        private final Optional<MetricValue> value;
    }

    @RequiredArgsConstructor
    @Getter
    private static class RuntimeIOException extends RuntimeException {
        private final IOException ex;
    }
}
