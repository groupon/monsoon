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
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter.Writer;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import static gnu.trove.TCollections.unmodifiableMap;
import java.io.Closeable;

public class GroupTable implements Closeable {
    private final DictionaryForWrite dictionary;
    private final TLongSet timestamps = new TLongHashSet();
    private final TIntObjectMap<FilePos> filePosTbl = new TIntObjectHashMap<>();
    private final MetricsTmpfile metrics;

    public GroupTable(@NonNull DictionaryForWrite dictionary) throws IOException {
        this.dictionary = dictionary;
        this.metrics = new MetricsTmpfile();
    }

    public void add(long ts, @NonNull Map<MetricName, MetricValue> metrics) throws IOException {
        timestamps.add(ts);
        for (Map.Entry<MetricName, MetricValue> entry : metrics.entrySet())
            this.metrics.add(ts, entry.getKey(), entry.getValue(), dictionary);
    }

    private TIntObjectMap<MetricTable> getTables() throws IOException {
        final TIntObjectMap<MetricTable> metricTbl = new TIntObjectHashMap<>();

        metrics.stream(dictionary.asDictionaryDelta())
                .forEach(entry -> {
                    final int mIdx = entry.getMetricPathRef();
                    MetricTable mt = metricTbl.get(mIdx);
                    if (mt == null) {
                        mt = new MetricTable(dictionary);
                        metricTbl.put(mIdx, mt);
                    }

                    mt.add(entry.getTimestamp(), entry.getMetricValue());
                });

        return unmodifiableMap(metricTbl);
    }

    private group_table encode(long timestamps[], TIntObjectMap<MetricTable> metricTbl) {
        group_table result = new group_table();
        result.presence = createPresenceBitset(this.timestamps, timestamps);
        result.metric_tbl = Arrays.stream(metricTbl.keys())
                .mapToObj(mIdx -> {
                    final tables_metric tm = new tables_metric();
                    tm.metric_ref = mIdx;
                    tm.pos = ToXdr.filePos(filePosTbl.get(mIdx));
                    return tm;
                })
                .toArray(tables_metric[]::new);

        return result;
    }

    public FilePos write(Writer writer, long timestamps[]) throws OncRpcException, IOException {
        final TIntObjectMap<MetricTable> metricTbl = getTables();
        for (int mIdx : metricTbl.keys()) {
            if (!filePosTbl.containsKey(mIdx)) {
                final MetricTable tbl = metricTbl.get(mIdx);
                final FilePos pos = tbl.write(writer, timestamps);
                filePosTbl.put(mIdx, pos);
            }
        }

        return writer.write(encode(timestamps, metricTbl));
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }

    public static class MetricsTmpfileException extends RuntimeException {
        public MetricsTmpfileException() {
        }

        public MetricsTmpfileException(String message) {
            super(message);
        }

        public MetricsTmpfileException(String message, Throwable cause) {
            super(message, cause);
        }

        public MetricsTmpfileException(Throwable cause) {
            super(cause);
        }

        public MetricsTmpfileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}
