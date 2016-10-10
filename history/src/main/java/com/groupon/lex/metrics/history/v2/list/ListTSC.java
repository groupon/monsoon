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
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.record_array;
import com.groupon.lex.metrics.history.v2.xdr.record_metrics;
import com.groupon.lex.metrics.history.v2.xdr.record_tags;
import com.groupon.lex.metrics.history.xdr.support.DecodingException;
import com.groupon.lex.metrics.history.xdr.support.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelSegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import gnu.trove.map.hash.THashMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.unmodifiableSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;

public class ListTSC extends AbstractTimeSeriesCollection {
    @Getter
    private final DateTime timestamp;
    private final SegmentReader<Map<GroupName, SegmentReader<TimeSeriesValue>>> data;

    private static <T> BinaryOperator<T> throwing_merger_() {
        return (x, y) -> { throw new IllegalStateException("duplicate key " + x); };
    }

    /** HashMap constructor, so we can create hashmaps with an altered load factor. */
    private static <K, V> Supplier<Map<K, V>> hashmap_constructor_() {
        return () -> new THashMap<K, V>(1, 1);
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
                .map(this::decode)
                .collect(Collectors.toList());
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        return new TimeSeriesValueSet(decode(data).entrySet().stream()
                .filter(entry -> Objects.equals(entry.getKey().getPath(), name))
                .map(Map.Entry::getValue)
                .map(this::decode));
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(decode(data).get(name))
                .map(this::decode);
    }

    @RequiredArgsConstructor
    private static class RecordArray {
        private final DateTime ts;
        private final record_array ra;
        private final FileChannelSegmentReader.Factory segmentFactory;

        public Map<GroupName, SegmentReader<TimeSeriesValue>> mapToTSData(DictionaryDelta dictionary) {
            return Arrays.stream(ra.value)
                    .flatMap(r -> {
                        final SimpleGroupPath path = SimpleGroupPath.valueOf(dictionary.getPath(r.path_ref));
                        return mapTags(path, r.tags, dictionary);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private Stream<Map.Entry<GroupName, SegmentReader<TimeSeriesValue>>> mapTags(SimpleGroupPath path, record_tags rta[], DictionaryDelta dictionary) {
            return Arrays.stream(rta)
                    .map(rt -> {
                        final Tags tags = dictionary.getTags(rt.tag_ref);
                        final GroupName group = GroupName.valueOf(path, tags);
                        final SegmentReader<TimeSeriesValue> tsv = segmentFactory.get(record_metrics::new, FromXdr.filePos(rt.pos))
                                .map(rm -> new GroupVal(group, rm))
                                .map(gv -> gv.mapToTsv(ts, dictionary))
                                .cache();
                        return SimpleMapEntry.create(group, tsv);
                    });
        }
    }

    @RequiredArgsConstructor
    private static class GroupVal {
        private final GroupName group;
        private final record_metrics metrics;

        public TimeSeriesValue mapToTsv(DateTime ts, DictionaryDelta dict) {
            return new ImmutableTimeSeriesValue(
                    ts,
                    group,
                    Arrays.stream(metrics.value),
                    r -> MetricName.valueOf(dict.getPath(r.path_ref)),
                    r -> FromXdr.metricValue(r.v, dict::getString));
        }
    }
}
