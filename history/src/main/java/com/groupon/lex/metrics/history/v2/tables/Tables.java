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

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.ExportMap;
import com.groupon.lex.metrics.history.v2.xdr.tables;
import com.groupon.lex.metrics.history.v2.xdr.tables_group;
import com.groupon.lex.metrics.history.v2.xdr.tables_tag;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;

@RequiredArgsConstructor
public class Tables {
    @NonNull
    private final DateTime begin;
    @NonNull
    private final ExportMap<List<String>> pathTable;
    @NonNull
    private final ExportMap<Tags> tagsTable;
    @NonNull
    private final ExportMap<String> stringTable;
    private final TIntObjectMap<GroupTbl> groups = new TIntObjectHashMap<>();

    public void add(TimeSeriesCollection tsdata) {
        add(tsdata.getTimestamp(), tsdata.getTSValues().stream());
    }

    public void add(DateTime ts, Stream<TimeSeriesValue> tsdata) {
        tsdata.forEach((tsv) -> add(ts, tsv));
    }

    public void add(DateTime ts, TimeSeriesValue tsv) {
        add(ts, tsv.getGroup().getPath(), tsv.getGroup().getTags(), tsv.getMetrics());
    }

    private void add(DateTime ts, SimpleGroupPath grpPath, Tags grpTags, Map<MetricName, MetricValue> metrics) {
        if (ts.isBefore(begin))
            throw new IllegalArgumentException("begin time violation");

        final int pathIdx = pathTable.getOrCreate(grpPath.getPath());
        GroupTbl dest = groups.get(pathIdx);
        if (dest == null) {
            dest = new GroupTbl(begin, pathTable, tagsTable, stringTable);
            groups.put(pathIdx, dest);
        }

        dest.add(ts, grpTags, metrics);
    }

    public tables encode() {
        return new tables(Arrays.stream(groups.keys())
                .mapToObj(pathIdx -> {
                    tables_group tg = new tables_group();
                    tg.group_ref = pathIdx;
                    tg.tag_tbl = groups.get(pathIdx).encode();
                    return tg;
                })
                .toArray(tables_group[]::new));
    }

    public long write(FileChannel out, long outPos, boolean compress) throws OncRpcException, IOException {
        for (GroupTbl group : groups.valueCollection())
            outPos = group.write(out, outPos, compress);

        return outPos;
    }

    @RequiredArgsConstructor
    private static class GroupTbl {
        @NonNull
        DateTime begin;
        @NonNull
        private final ExportMap<List<String>> pathTable;
        @NonNull
        private final ExportMap<Tags> tagsTable;
        @NonNull
        private final ExportMap<String> stringTable;
        private final TIntObjectMap<GroupTable> t_group = new TIntObjectHashMap<>();
        private final TIntObjectMap<FilePos> filePosTbl = new TIntObjectHashMap<>();

        public void add(DateTime ts, Tags grpTags, Map<MetricName, MetricValue> metrics) {
            final int tagIdx = tagsTable.getOrCreate(grpTags);
            GroupTable dest = t_group.get(tagIdx);
            if (dest == null) {
                dest = new GroupTable(begin, pathTable, stringTable);
                t_group.put(tagIdx, dest);
            }

            dest.add(ts, metrics);
        }

        public tables_tag[] encode() {
            return Arrays.stream(t_group.keys())
                    .mapToObj(tagIdx -> {
                        tables_tag tt = new tables_tag();
                        tt.tag_ref = tagIdx;
                        tt.pos = filePosTbl.get(tagIdx).encode();
                        return tt;
                    })
                    .toArray(tables_tag[]::new);
        }

        public long write(FileChannel out, long outPos, boolean compress) throws OncRpcException, IOException {
            for (int tagIdx : t_group.keys()) {
                if (!filePosTbl.containsKey(tagIdx)) {
                    final FilePos pos = t_group.get(tagIdx).write(out, outPos, compress);
                    filePosTbl.put(tagIdx, pos);
                    outPos = pos.getEnd();
                }
            }
            return outPos;
        }
    }
}
