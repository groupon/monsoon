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
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.ToXdr.createPresenceBitset;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.tables_metric;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter;
import static gnu.trove.TCollections.unmodifiableMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import static gnu.trove.TCollections.unmodifiableMap;

@RequiredArgsConstructor
public class GroupTable extends AbstractSegmentWriter {
    @NonNull
    private final DictionaryForWrite dictionary;
    private final TLongSet timestamps = new TLongHashSet();
    private final TIntObjectMap<FilePos> filePosTbl = new TIntObjectHashMap<>();
    private final TIntObjectMap<MetricTable> metricTbl = new TIntObjectHashMap<>();

    public void add(long ts, @NonNull Map<MetricName, MetricValue> metrics) {
        timestamps.add(ts);
        metrics.entrySet().forEach(entry -> {
            final int mIdx = dictionary.getPathTable().getOrCreate(entry.getKey().getPath());
            MetricTable mt = metricTbl.get(mIdx);
            if (mt == null) {
                mt = new MetricTable(dictionary);
                metricTbl.put(mIdx, mt);
            }

            mt.add(ts, entry.getValue());
        });
    }

    public TIntObjectMap<MetricTable> getTables() {
        return unmodifiableMap(metricTbl);
    }

    @Override
    public group_table encode(long timestamps[]) {
        group_table result = new group_table();
        result.presence = createPresenceBitset(this.timestamps, timestamps);
        result.metric_tbl = Arrays.stream(metricTbl.keys())
                .mapToObj(mIdx -> {
                    final tables_metric tm = new tables_metric();
                    tm.metric_ref = mIdx;
                    tm.pos = ToXdr.filePos(filePosTbl.get(mIdx));
                    return result;
                })
                .toArray(tables_metric[]::new);

        return result;
    }

    @Override
    public FilePos write(Writer writer, long timestamps[]) throws OncRpcException, IOException {
        for (int mIdx : metricTbl.keys()) {
            if (!filePosTbl.containsKey(mIdx)) {
                final MetricTable tbl = metricTbl.get(mIdx);
                final FilePos pos = tbl.write(writer, timestamps);
                filePosTbl.put(mIdx, pos);
            }
        }

        return super.write(writer, timestamps);
    }
}
