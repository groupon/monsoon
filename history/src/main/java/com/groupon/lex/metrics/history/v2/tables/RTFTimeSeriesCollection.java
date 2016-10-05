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
package com.groupon.lex.metrics.history.v2.tables;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import java.io.IOException;
import java.util.Collection;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class RTFTimeSeriesCollection extends AbstractTimeSeriesCollection {
    private final long ts;
    private final Map<SimpleGroupPath, Map<Tags, SegmentReader<Optional<RTFTimeSeriesValue>>>> groups;

    public RTFTimeSeriesCollection(int index, RTFFileDataTables data) {
        this.ts = data.getTimestamps()[index];
        this.groups = applyGroups(ts, index, data.getGroups());
    }

    private static Map<SimpleGroupPath, Map<Tags, SegmentReader<Optional<RTFTimeSeriesValue>>>> applyGroups(long ts, int index, Map<SimpleGroupPath, Map<Tags, SegmentReader<RTFGroupTable>>> groups) {
        return unmodifiableMap(groups.entrySet().stream()
                .collect(Collectors.toMap(
                        tagEntry -> tagEntry.getKey(),
                        tagEntry -> applyGroupsInner(ts, index, tagEntry.getKey(), tagEntry.getValue()))));
    }

    private static Map<Tags, SegmentReader<Optional<RTFTimeSeriesValue>>> applyGroupsInner(long ts, int index, SimpleGroupPath path, Map<Tags, SegmentReader<RTFGroupTable>> tags) {
        return unmodifiableMap(tags.entrySet().stream()
                .collect(Collectors.toMap(
                        tagEntry -> tagEntry.getKey(),
                        tagEntry -> applyGroupsFactory(ts, index, path, tagEntry.getKey(), tagEntry.getValue()))));
    }

    private static SegmentReader<Optional<RTFTimeSeriesValue>> applyGroupsFactory(long ts, int index, SimpleGroupPath path, Tags tags, SegmentReader<RTFGroupTable> segment) {
        return segment
                .filter((gt -> gt.contains(index)))
                .map(optGt -> optGt.map(gt -> new RTFTimeSeriesValue(ts, index, GroupName.valueOf(path, tags), gt)))
                .cache();
    }

    @Override
    public TimeSeriesCollection add(TimeSeriesValue tsv) {
        throw new UnsupportedOperationException("History is read only.");
    }

    @Override
    public TimeSeriesCollection renameGroup(GroupName oldname, GroupName newname) {
        throw new UnsupportedOperationException("History is read only.");
    }

    @Override
    public TimeSeriesCollection addMetrics(GroupName group, Map<MetricName, MetricValue> metrics) {
        throw new UnsupportedOperationException("History is read only.");
    }

    @Override
    public DateTime getTimestamp() {
        return new DateTime(ts, DateTimeZone.UTC);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Set<GroupName> getGroups() {
        return groups.entrySet().stream()
                .flatMap(groupEntry -> {
                    return groupEntry.getValue().entrySet().stream()
                            .map(tagsEntry -> SimpleMapEntry.create(GroupName.valueOf(groupEntry.getKey(), tagsEntry.getKey()), tagsEntry.getValue()));
                })
                .filter(entry -> {
                    try {
                        return entry.getValue().decode().isPresent();
                    } catch (IOException | OncRpcException ex) {
                        throw new RuntimeException("decoding error", ex);
                    }
                })
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet());
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths() {
        return groups.entrySet().stream()
                .filter(grpEntry -> {
                    return grpEntry.getValue().values().stream()
                            .map(segment -> {
                                try {
                                    return segment.decode();
                                } catch (IOException | OncRpcException ex) {
                                    throw new RuntimeException("decoding error", ex);
                                }
                            })
                            .anyMatch(Optional::isPresent);
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        return groups.values().stream()
                .flatMap(tagMap -> tagMap.values().stream())
                .flatMap(segment -> {
                    try {
                        return segment.decode().map(Stream::of).orElseGet(Stream::empty);
                    } catch (IOException | OncRpcException ex) {
                        throw new RuntimeException("decoding error", ex);
                    }
                })
                .collect(Collectors.toList());
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        final DateTime ts = getTimestamp();

        return new TimeSeriesValueSet(groups.getOrDefault(name, emptyMap()).values().stream()
                .flatMap(tsvResolver -> {
                    try {
                        return tsvResolver.decode().map(Stream::of).orElseGet(Stream::empty);
                    } catch (IOException | OncRpcException ex) {
                        throw new RuntimeException("decoding error", ex);
                    }
                }));
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(groups.get(name.getPath()))
                .flatMap(tagMap -> Optional.ofNullable(tagMap.get(name.getTags())))
                .flatMap(tsvResolver -> {
                    try {
                        return tsvResolver.decode();
                    } catch (IOException | OncRpcException ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .map(x -> x);  // Fixing compiler's idea of the type.
    }

    @Override
    public RTFTimeSeriesCollection clone() { return this; }  // Read-only doesn't need clone.
}
