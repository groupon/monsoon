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
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import com.groupon.lex.metrics.history.v2.xdr.tables;
import com.groupon.lex.metrics.history.v2.xdr.tables_group;
import com.groupon.lex.metrics.history.v2.xdr.tables_tag;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter.Writer;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;

@RequiredArgsConstructor
public class Tables extends AbstractSegmentWriter implements Closeable {
    @NonNull
    private final DictionaryForWrite dictionary;
    private final TIntObjectMap<GroupTbl> groups = new TIntObjectHashMap<>();

    @Override
    public void close() {
        groups.forEachValue(grpTbl -> {
            try {
                grpTbl.close();
            } catch (IOException ex) {
                Logger.getLogger(GroupTbl.class.getName()).log(Level.WARNING, "unable to close groups", ex);
                /* Let Garbage Collector deal with dangling FileChannel. */
            }
            return true;
        });
    }

    public void add(long ts, Collection<TimeSeriesValue> tsdata) throws IOException {
        for (TimeSeriesValue tsv : tsdata)
            add(ts, tsv);
    }

    public void add(long ts, TimeSeriesValue tsv) throws IOException {
        add(ts, tsv.getGroup().getPath(), tsv.getGroup().getTags(), tsv.getMetrics());
    }

    private void add(long ts, SimpleGroupPath grpPath, Tags grpTags, Map<MetricName, MetricValue> metrics) throws IOException {
        final int pathIdx = dictionary.getPathTable().getOrCreate(grpPath.getPath());
        GroupTbl dest = groups.get(pathIdx);
        if (dest == null) {
            dest = new GroupTbl(dictionary);
            groups.put(pathIdx, dest);
        }

        dest.add(ts, grpTags, metrics);
    }

    @Override
    public tables encode(long timestamps[]) {
        return new tables(Arrays.stream(groups.keys())
                .mapToObj(pathIdx -> {
                    tables_group tg = new tables_group();
                    tg.group_ref = pathIdx;
                    tg.tag_tbl = groups.get(pathIdx).encode();
                    return tg;
                })
                .toArray(tables_group[]::new));
    }

    @Override
    public FilePos write(Writer writer, long timestamps[]) throws OncRpcException, IOException {
        for (GroupTbl group : groups.valueCollection())
            group.write(writer, timestamps);

        return super.write(writer, timestamps);
    }

    @RequiredArgsConstructor
    private static class GroupTbl implements Closeable {
        @NonNull
        private final DictionaryForWrite dictionary;
        private final TIntObjectMap<GroupTable> t_group = new TIntObjectHashMap<>();
        private final TIntObjectMap<FilePos> filePosTbl = new TIntObjectHashMap<>();

        @Override
        public void close() throws IOException {
            t_group.forEachValue(grpTbl -> {
                try {
                    grpTbl.close();
                } catch (IOException ex) {
                    Logger.getLogger(GroupTbl.class.getName()).log(Level.WARNING, "unable to close metrics", ex);
                    /* Let Garbage Collector deal with dangling FileChannel. */
                }
                return true;
            });
        }

        public void add(long ts, Tags grpTags, Map<MetricName, MetricValue> metrics) throws IOException {
            final int tagIdx = dictionary.getTagsTable().getOrCreate(grpTags);
            GroupTable dest = t_group.get(tagIdx);
            if (dest == null) {
                dest = new GroupTable(dictionary);
                t_group.put(tagIdx, dest);
            }

            dest.add(ts, metrics);
        }

        public tables_tag[] encode() {
            return Arrays.stream(t_group.keys())
                    .mapToObj(tagIdx -> {
                        tables_tag tt = new tables_tag();
                        tt.tag_ref = tagIdx;
                        tt.pos = ToXdr.filePos(filePosTbl.get(tagIdx));
                        return tt;
                    })
                    .toArray(tables_tag[]::new);
        }

        public void write(Writer writer, long timestamps[]) throws OncRpcException, IOException {
            for (int tagIdx : t_group.keys()) {
                if (!filePosTbl.containsKey(tagIdx)) {
                    final FilePos pos = t_group.get(tagIdx).write(writer, timestamps);
                    filePosTbl.put(tagIdx, pos);
                }
            }
        }
    }
}
