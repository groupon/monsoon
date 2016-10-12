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
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables_block;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.tables;
import com.groupon.lex.metrics.history.v2.xdr.tables_group;
import com.groupon.lex.metrics.history.xdr.support.DecodingException;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import gnu.trove.map.hash.THashMap;
import java.util.Arrays;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.acplt.oncrpc.XdrAble;

@Getter
public class RTFFileDataTablesBlock {
    private final long timestamps[];
    private final SegmentReader<Map<SimpleGroupPath, Map<GroupName, SegmentReader<RTFGroupTable>>>> table;
    private final ObjectSequence<TimeSeriesCollection> tsdata;

    public RTFFileDataTablesBlock(file_data_tables_block input, SegmentReader.Factory<XdrAble> segmentFactory) {
        this.timestamps = FromXdr.timestamp_delta(input.tsd);
        final SegmentReader<DictionaryDelta> dictionarySegment = segmentFactory.get(dictionary_delta::new, FromXdr.filePos(input.dictionary))
                .map(DictionaryDelta::new)
                .cache();
        this.table = segmentFactory.get(tables::new, FromXdr.filePos(input.tables_data))
                .combine(dictionarySegment, (xdrTables, dict) -> outerMap(xdrTables, dict, dictionarySegment, segmentFactory))
                .cache();
        this.tsdata = new ForwardSequence(0, this.timestamps.length)
                .map(idx -> newTSC(idx), true, true, true)
                .share();
    }

    public int size() {
        return timestamps.length;
    }

    public void validate() {
    }

    private TimeSeriesCollection newTSC(int idx) {
        return new RTFTimeSeriesCollection(idx, timestamps[idx], table);
    }

    public Set<SimpleGroupPath> getAllPaths() {
        return unmodifiableSet(table.decodeOrThrow().keySet());
    }

    public Set<GroupName> getAllNames() {
        return table.decodeOrThrow().values().stream()
                .flatMap(grpMap -> grpMap.keySet().stream())
                .collect(Collectors.toSet());
    }

    private static Map<SimpleGroupPath, Map<GroupName, SegmentReader<RTFGroupTable>>> outerMap(tables xdrTables, DictionaryDelta dictionary, SegmentReader<DictionaryDelta> dictionarySegment, SegmentReader.Factory<XdrAble> segmentFactory) {
        return Arrays.stream(xdrTables.value)
                .map(tg -> {
                    final SimpleGroupPath path = SimpleGroupPath.valueOf(dictionary.getPath(tg.group_ref));
                    final Map<GroupName, SegmentReader<RTFGroupTable>> groups = unmodifiableMap(innerMap(path, tg, dictionary, dictionarySegment, segmentFactory));
                    return SimpleMapEntry.create(path, groups);
                })
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> {
                            throw new DecodingException("duplicate group reference");
                        },
                        () -> new THashMap<>(1, 1)));
    }

    private static Map<GroupName, SegmentReader<RTFGroupTable>> innerMap(SimpleGroupPath path, tables_group tg, DictionaryDelta dictionary, SegmentReader<DictionaryDelta> dictionarySegment, SegmentReader.Factory<XdrAble> segmentFactory) {
        return Arrays.stream(tg.tag_tbl)
                .collect(Collectors.toMap(
                        tt -> GroupName.valueOf(path, dictionary.getTags(tt.tag_ref)),
                        tt -> segmentFromFilePos(FromXdr.filePos(tt.pos), dictionarySegment, segmentFactory),
                        (a, b) -> {
                            throw new DecodingException("duplicate tag reference");
                        },
                        () -> new THashMap<>(1, 1)));
    }

    private static SegmentReader<RTFGroupTable> segmentFromFilePos(FilePos fp, SegmentReader<DictionaryDelta> dictionarySegment, SegmentReader.Factory<XdrAble> segmentFactory) {
        return segmentFactory.get(group_table::new, fp)
                .combine(dictionarySegment, (gt, dictionary) -> new RTFGroupTable(gt, dictionary, segmentFactory))
                .peek(RTFGroupTable::validate)
                .share();
    }
}
