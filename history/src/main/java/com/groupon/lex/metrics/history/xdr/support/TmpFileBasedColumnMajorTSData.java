package com.groupon.lex.metrics.history.xdr.support;

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
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.synchronizedSet;
import static java.util.Collections.unmodifiableSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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
    private static final Compression TMP_FILE_COMPRESSION = Compression.SNAPPY;
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
                                        (g) -> {
                                            try {
                                                return new GroupWriter();
                                            } catch (IOException ex) {
                                                throw new RuntimeIOException(ex);
                                            }
                                        });

                                try {
                                    fixBacklog(groupWriter);

                                    assert groupWriter.size() == timestamps.size();
                                    groupWriter.add(tsv.getMetrics());
                                } catch (OncRpcException ex) {
                                    throw new RuntimeIOException(new IOException("encoding problem", ex));
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
        private void fixBacklog() throws OncRpcException, IOException {
            for (GroupWriter groupWriter : writers.values()) {
                fixBacklog(groupWriter);
            }
        }

        /**
         * Fill in the backlog. Backlog can occur if a group is not always
         * present, causing us to miss writes that should occur.
         */
        private void fixBacklog(GroupWriter groupWriter) throws OncRpcException, IOException {
            for (int backlogIdx = groupWriter.size();
                 backlogIdx < timestamps.size();
                 ++backlogIdx)
                groupWriter.add(emptyMap());
        }

        public TmpFileBasedColumnMajorTSData build() throws IOException {
            try {
                fixBacklog();

                final Map<GroupName, Group> groups = writers.entrySet().stream()
                        .peek(groupWriterEntry -> {
                            assert groupWriterEntry.getValue().size() == timestamps.size();
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, groupWriterEntry -> groupWriterEntry.getValue().asReader()));
                return new TmpFileBasedColumnMajorTSData(timestamps, groups, timestampsByGroup);
            } catch (OncRpcException ex) {
                throw new IOException("encoding problem", ex);
            }
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
        if (groupData == null)
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
                return StreamSupport.stream(Spliterators.spliteratorUnknownSize(groupData.iterator(timestamps), 0), false)
                        .map(timestampedTsv -> {
                            final MetricValue metricValue = timestampedTsv.getTsv().get(metric);
                            if (metricValue == null) return null;
                            return SimpleMapEntry.create(timestampedTsv.getTimestamp(), metricValue);
                        })
                        .filter(entry -> entry != null)
                        .iterator();
            }

            @Override
            public int size() {
                return timestamps.size();
            }
        }
    }

    private static class GroupWriter {
        private final Set<MetricName> metricNames = new HashSet<>();
        private final GCCloseable<TmpFile<XdrAbleMetricMap>> tmpFile;
        private final DictionaryForWrite dictionary = new DictionaryForWrite();

        public GroupWriter() throws IOException {
            this.tmpFile = new GCCloseable<>(new TmpFile<>(TMP_FILE_COMPRESSION));
        }

        public void add(Map<MetricName, MetricValue> tsv) throws OncRpcException, IOException {
            metricNames.addAll(tsv.keySet());
            tmpFile.get().add(new XdrAbleMetricMap(dictionary, tsv));
        }

        public int size() {
            return tmpFile.get().size();
        }

        public Group asReader() {
            return new Group(metricNames, tmpFile);
        }
    }

    @AllArgsConstructor
    private static class Group {
        @Getter
        private final Set<MetricName> metricNames;
        private final GCCloseable<TmpFile<XdrAbleMetricMap>> tmpFile;

        public Iterator<TimestampedTsv> iterator(TLongList timestamps) {
            return new IteratorImpl(tmpFile, timestamps);
        }

        private static class IteratorImpl implements Iterator<TimestampedTsv> {
            private final TLongIterator timestampIter;
            private final Iterator<XdrAbleMetricMap> inner;
            private DictionaryDelta dictionary = new DictionaryDelta();

            /*
             * Bind the lifetime of tmpFile to the lifetime of this iterator.
             */
            private final GCCloseable<TmpFile<XdrAbleMetricMap>> tmpFile;

            public IteratorImpl(@NonNull GCCloseable<TmpFile<XdrAbleMetricMap>> tmpFile, @NonNull TLongList timestamps) {
                this.tmpFile = tmpFile;
                this.timestampIter = timestamps.iterator();
                try {
                    inner = this.tmpFile.get().iterator(XdrAbleMetricMap::new);
                } catch (IOException | OncRpcException ex) {
                    throw new DecodingException("cannot read: decoding failed", ex);
                }
            }

            @Override
            public boolean hasNext() {
                return inner.hasNext();
            }

            @Override
            public TimestampedTsv next() {
                final DateTime ts = new DateTime(timestampIter.next(), DateTimeZone.UTC);
                final Map<MetricName, MetricValue> metricMap = inner.next()
                        .decode(dictionary, (updatedDictionary) -> dictionary = updatedDictionary);
                return new TimestampedTsv(ts, metricMap);
            }
        }
    }

    private static class XdrAbleMetricMap implements XdrAble {
        private TIntObjectMap<metric_value> metrics;
        private dictionary_delta dd;

        public XdrAbleMetricMap() {
            /* SKIP */
        }

        public XdrAbleMetricMap(@NonNull DictionaryForWrite dictionary, @NonNull Map<MetricName, MetricValue> metrics) {
            this.metrics = new TIntObjectHashMap<>();

            metrics.entrySet().stream()
                    .forEach(entry -> {
                        final int metricName = dictionary.getPathTable().getOrCreate(entry.getKey().getPath());
                        final metric_value metricValue = ToXdr.metricValue(entry.getValue(), dictionary.getStringTable()::getOrCreate);
                        this.metrics.put(metricName, metricValue);
                    });

            this.dd = dictionary.encode();
            dictionary.reset();
        }

        public Map<MetricName, MetricValue> decode(DictionaryDelta inputDictionary, Consumer<DictionaryDelta> updateDictionary) {
            final Map<MetricName, MetricValue> metricMap = new HashMap<>();
            final DictionaryDelta dictionary = new DictionaryDelta(dd, inputDictionary);

            metrics.forEachEntry((metricName, metricValue) -> {
                metricMap.put(MetricName.valueOf(dictionary.getPath(metricName)),
                        FromXdr.metricValue(metricValue, dictionary::getString));
                return true;
            });

            updateDictionary.accept(dictionary);
            return metricMap;
        }

        @Override
        public void xdrEncode(XdrEncodingStream stream) throws OncRpcException, IOException {
            stream.xdrEncodeInt(metrics.size());

            final TIntObjectIterator<metric_value> iter = metrics.iterator();
            while (iter.hasNext()) {
                iter.advance();
                stream.xdrEncodeInt(iter.key());
                iter.value().xdrEncode(stream);
            }

            dd.xdrEncode(stream);
        }

        @Override
        public void xdrDecode(XdrDecodingStream stream) throws OncRpcException, IOException {
            final int sz = stream.xdrDecodeInt();

            metrics = new TIntObjectHashMap<>();
            for (int i = 0; i < sz; ++i) {
                final int metricName = stream.xdrDecodeInt();
                final metric_value metricValue = new metric_value(stream);
                metrics.put(metricName, metricValue);
            }

            dd = new dictionary_delta(stream);
        }
    }

    @Value
    private static class TimestampedTsv {
        @NonNull
        private final DateTime timestamp;
        @NonNull
        private final Map<MetricName, MetricValue> tsv;
    }

    @RequiredArgsConstructor
    @Getter
    private static class RuntimeIOException extends RuntimeException {
        private final IOException ex;
    }
}
