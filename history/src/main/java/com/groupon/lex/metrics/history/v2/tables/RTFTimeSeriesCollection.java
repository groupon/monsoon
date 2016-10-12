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
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import java.util.Collection;
import static java.util.Collections.emptyMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

@RequiredArgsConstructor
public class RTFTimeSeriesCollection extends AbstractTimeSeriesCollection {
    private final int index;
    private final long timestamp;
    private final SegmentReader<Map<SimpleGroupPath, Map<GroupName, SegmentReader<RTFGroupTable>>>> table;

    @Override
    public DateTime getTimestamp() {
        return new DateTime(timestamp, DateTimeZone.UTC);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Set<GroupName> getGroups() {
        return table.decodeOrThrow().values().stream()
                .flatMap(grpMap -> grpMap.entrySet().stream())
                .filter(groupEntry -> groupEntry.getValue().decodeOrThrow().contains(index))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<SimpleGroupPath> getGroupPaths() {
        return table.decodeOrThrow().entrySet().stream()
                .filter(pathMap -> {
                    Map<GroupName, SegmentReader<RTFGroupTable>> grpMap = pathMap.getValue();
                    return grpMap.values().stream()
                            .anyMatch(grp -> grp.decodeOrThrow().contains(index));
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private TimeSeriesValue newTSV(GroupName group, RTFGroupTable table) {
        return new RTFTimeSeriesValue(timestamp, index, group, table);
    }

    @Override
    public Collection<TimeSeriesValue> getTSValues() {
        return table.decodeOrThrow().values().stream()
                .flatMap(grpMap -> grpMap.entrySet().stream())
                .map(grpEntry -> SimpleMapEntry.create(grpEntry.getKey(), grpEntry.getValue().decodeOrThrow()))
                .filter(grpEntry -> grpEntry.getValue().contains(index))
                .map(grpEntry -> newTSV(grpEntry.getKey(), grpEntry.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public TimeSeriesValueSet getTSValue(SimpleGroupPath name) {
        return new TimeSeriesValueSet(table.decodeOrThrow().get(name).entrySet().stream()
                .map(grpEntry -> SimpleMapEntry.create(grpEntry.getKey(), grpEntry.getValue().decodeOrThrow()))
                .filter(grpEntry -> grpEntry.getValue().contains(index))
                .map(grpEntry -> newTSV(grpEntry.getKey(), grpEntry.getValue())));
    }

    @Override
    public Optional<TimeSeriesValue> get(GroupName name) {
        return Optional.ofNullable(table.decodeOrThrow().getOrDefault(name.getPath(), emptyMap()).get(name))
                .map(grpSegment -> grpSegment.decodeOrThrow())
                .filter(grpTbl -> grpTbl.contains(index))
                .map(grpTbl -> newTSV(name, grpTbl));
    }
}
