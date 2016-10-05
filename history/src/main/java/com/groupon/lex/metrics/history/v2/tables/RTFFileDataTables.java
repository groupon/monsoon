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
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.tables_group;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.Sequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.Arrays;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import org.acplt.oncrpc.XdrAble;
import org.joda.time.DateTime;

@Getter(AccessLevel.PACKAGE)
public final class RTFFileDataTables {
    private final long[] timestamps;
    private final Map<SimpleGroupPath, Map<Tags, SegmentReader<RTFGroupTable>>> groups;

    public RTFFileDataTables(file_data_tables input, long begin, SegmentReader.Factory segmentFactory) {
        final DictionaryDelta dictionary = new DictionaryDelta(input.dictionary);
        this.timestamps = FromXdr.timestamp_delta(begin, input.tsd);
        this.groups = unmodifiableMap(outerMap(input.tables_data.value, begin, dictionary, segmentFactory));
    }

    private static long[] timestamps(int[] ts_delta, DateTime begin) {
        long ts = begin.getMillis();
        final long result[] = new long[ts_delta.length];
        for (int i = 0; i < ts_delta.length; ++i) {
            ts = ts += ts_delta[i];
            result[i] = ts;
        }
        return result;
    }

    private static Map<SimpleGroupPath, Map<Tags, SegmentReader<RTFGroupTable>>> outerMap(tables_group tgArray[], DateTime begin, DictionaryDelta dictionary, SegmentReader.Factory<XdrAble> segmentFactory) {
        return Arrays.stream(tgArray)
                .collect(Collectors.toMap(
                        tg -> SimpleGroupPath.valueOf(dictionary.getPath(tg.group_ref)),
                        tg -> unmodifiableMap(innerMap(tg, begin, dictionary, segmentFactory))));
    }

    private static Map<Tags, SegmentReader<RTFGroupTable>> innerMap(tables_group tg, DateTime begin, DictionaryDelta dictionary, SegmentReader.Factory<XdrAble> segmentFactory) {
        return Arrays.stream(tg.tag_tbl)
                .collect(Collectors.toMap(
                        tt -> dictionary.getTags(tt.tag_ref),
                        tt -> segmentFromFilePos(new FilePos(tt.pos), begin, dictionary, segmentFactory)
                ));
    }

    private static SegmentReader<RTFGroupTable> segmentFromFilePos(FilePos fp, DateTime begin, DictionaryDelta dictionary, SegmentReader.Factory<XdrAble> segmentFactory) {
        return segmentFactory.get(group_table::new, fp)
                .map(gt -> new RTFGroupTable(gt, begin, dictionary, segmentFactory))
                .share();
    }

    public int size() { return timestamps.length; }

    public Stream<TimeSeriesCollection> stream() {
        return new Sequence(0, timestamps.length).stream()
                .mapToObj(index -> new RTFTimeSeriesCollection(index, this));
    }

    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        int start = Arrays.binarySearch(timestamps, begin.getMillis());
        if (start < 0) start = -(start + 1);
        return new Sequence(start, timestamps.length).stream()
                .mapToObj(index -> new RTFTimeSeriesCollection(index, this));
    }

    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        int start = Arrays.binarySearch(timestamps, begin.getMillis());
        if (start < 0) start = -(start + 1);

        int stop = Arrays.binarySearch(timestamps, end.getMillis());
        if (stop < 0)
            stop = -(stop + 1);
        else
            stop++;  // stream should include exact match

        return new Sequence(start, stop).stream()
                .mapToObj(index -> new RTFTimeSeriesCollection(index, this));
    }

    public Set<SimpleGroupPath> getAllPaths() { return groups.keySet(); }
    public Set<GroupName> getAllNames() {
        return groups.entrySet().stream()
                .flatMap(pathEntry -> {
                    return pathEntry.getValue().keySet().stream()
                            .map(tags -> GroupName.valueOf(pathEntry.getKey(), tags));
                })
                .collect(Collectors.toSet());
    }

    public SegmentReader<RTFGroupTable> getGroupTable(GroupName group) {
        return groups.getOrDefault(group.getPath(), emptyMap())
                .get(group.getTags());
    }
}
