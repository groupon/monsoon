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
package com.groupon.lex.metrics.history.v2.list;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.record_array;
import com.groupon.lex.metrics.history.v2.xdr.record_metrics;
import com.groupon.lex.metrics.history.v2.xdr.record_tags;
import com.groupon.lex.metrics.history.xdr.support.DecodingException;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelSegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import gnu.trove.map.hash.THashMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;

public class ListTSC extends AbstractTimeSeriesCollection {
    @Getter
    private final DateTime timestamp;
    private final SegmentReader<Map<GroupName, TimeSeriesValue>> data;

    private static <T> BinaryOperator<T> throwing_merger_() {
        return (x, y) -> { throw new IllegalStateException("duplicate key " + x); };
    }

    /** HashMap constructor, so we can create hash maps with an altered load factor. */
    private static <K, V> Supplier<Map<K, V>> hashmap_constructor_() {
        return () -> new THashMap<>(1, 1);
    }

    public ListTSC(DateTime ts, SegmentReader<record_array> init, SegmentReader<DictionaryDelta> dict, FileChannelSegmentReader.Factory segmentFactory) {
        timestamp = ts;
        data = init.map(ra -> new RecordArray(timestamp, ra, segmentFactory))
                .combine(dict, RecordArray::mapToTSData)
                .cache();
    }

    private <T> T decode(SegmentReader<T> sr) {
        try {
            return sr.decode();
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("unable to extract metrics for " + timestamp, ex);
        }
    }

    @Override
    public boolean isEmpty() {
        return decode(data).isEmpty();
    }

    @Override
    public Set<GroupName> getGroups() {
        return unmodifiableSet(decode(data).keySet());
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths() {
        return decode(data).keySet().stream()
                .map(GroupName::getPath)
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        return decode(data.map(Map::values)).stream()
                .collect(Collectors.toList());
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        return new TimeSeriesValueSet(decode(data).entrySet().stream()
                .filter(entry -> Objects.equals(entry.getKey().getPath(), name))
                .map(Map.Entry::getValue));
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(decode(data).get(name));
    }

    @RequiredArgsConstructor
    private static class RecordArray {
        private final DateTime ts;
        private final record_array ra;
        private final FileChannelSegmentReader.Factory segmentFactory;

        public Map<GroupName, TimeSeriesValue> mapToTSData(DictionaryDelta dictionary) {
            return Arrays.stream(ra.value)
                    .flatMap(r -> {
                        final SimpleGroupPath path = SimpleGroupPath.valueOf(dictionary.getPath(r.path_ref));
                        return mapTags(path, r.tags, dictionary);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, throwing_merger_(), hashmap_constructor_()));
        }

        private Stream<Map.Entry<GroupName, TimeSeriesValue>> mapTags(SimpleGroupPath path, record_tags rta[], DictionaryDelta dictionary) {
            return Arrays.stream(rta)
                    .map(rt -> {
                        final Tags tags = dictionary.getTags(rt.tag_ref);
                        final GroupName group = GroupName.valueOf(path, tags);
                        final TimeSeriesValue tsv = new ListTimeSeriesValue(
                                ts,
                                group,
                                segmentFactory.get(record_metrics::new, FromXdr.filePos(rt.pos))
                                        .map(metrics -> {
                                            return Arrays.stream(metrics.value)
                                                    .collect(Collectors.toMap(r -> MetricName.valueOf(dictionary.getPath(r.path_ref)), r -> FromXdr.metricValue(r.v, dictionary::getString), throwing_merger_(), hashmap_constructor_()));
                                        })
                                        .cache());
                        return SimpleMapEntry.create(group, tsv);
                    });
        }
    }

    @AllArgsConstructor
    private static class ListTimeSeriesValue extends AbstractTimeSeriesValue {
        @Getter
        private final DateTime timestamp;
        @Getter
        private final GroupName group;
        private final SegmentReader<Map<MetricName, MetricValue>> data;

        @Override
        public Map<MetricName, MetricValue> getMetrics() {
            return unmodifiableMap(decode(data));
        }

        @Override
        public TimeSeriesValue clone() {
            return this;  // Read-only needs no copy.
        }

        private <T> T decode(SegmentReader<T> sr) {
            try {
                return sr.decode();
            } catch (IOException | OncRpcException ex) {
                throw new DecodingException("unable to extract metrics for " + timestamp, ex);
            }
        }
    }
}
