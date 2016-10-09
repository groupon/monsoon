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
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.ToXdr.createPresenceBitset;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tables;
import com.groupon.lex.metrics.history.v2.xdr.tables_group;
import com.groupon.lex.metrics.history.v2.xdr.tables_metric;
import com.groupon.lex.metrics.history.v2.xdr.tables_tag;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.Const.writeMimeHeader;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter.Writer;
import static com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter.CRC_LEN;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTimeZone;

/**
 * Stateful writer for table-based file.
 *
 * Table based files have to be written in one go, as they can't be written
 * before all data has been gathered.
 * @author ariane
 */
public class ToXdrTables implements Closeable {
    private static final long HDR_SPACE = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
    private final TLongObjectMap<TimeSeriesCollection> tsdata = new TLongObjectHashMap<>();
    private final DictionaryForWrite dict = new DictionaryForWrite();

    public void add(TimeSeriesCollection tsc) {
        tsdata.put(tsc.getTimestamp().toDateTime(DateTimeZone.UTC).getMillis(), tsc);
    }

    public void addAll(Collection<? extends TimeSeriesCollection> tsc) {
        tsc.forEach(this::add);
    }

    public void write(@NonNull FileChannel out, boolean compress) throws OncRpcException, IOException {
        writeFile(out, compress);
    }

    @Override
    public void close() throws IOException {}

    private void writeFile(FileChannel out, boolean compress) throws IOException, OncRpcException {
        if (tsdata.isEmpty()) throw new IllegalStateException("table files may not be empty");
        final Context ctx = new Context(out, compress);

        final FilePos bodyPos;
        final long fileEnd;
        try (FileChannelWriter fd = new FileChannelWriter(out, HDR_SPACE)) {
            bodyPos = writeBody(ctx);
            fileEnd = fd.getOffset();
        }

        try (XdrEncodingFileWriter xdr = new XdrEncodingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(out, 0), HDR_SPACE), ctx.getUseBuffer())) {
            xdr.beginEncoding();
            writeMimeHeader(xdr);
            encodeHeader(bodyPos, fileEnd, ctx).xdrEncode(xdr);
            xdr.endEncoding();
        }
    }

    private FilePos writeBody(Context ctx) throws IOException, OncRpcException {
        file_data_tables result = new file_data_tables();
        result.tsd = ToXdr.timestamp_delta(ctx.getBegin(), ctx.getTimestamps());
        result.tables_data = createTables(ctx);  // Writes dependency data.
        result.dictionary = dict.encode();

        return ctx.newWriter().write(result);
    }

    private tables createTables(Context ctx) throws IOException, OncRpcException {
        TIntObjectMap<tables_tag[]> groupPathMap = new TIntObjectHashMap<>();

        // Write dependency data.
        final TLongObjectIterator<TimeSeriesCollection> tsdataIter = tsdata.iterator();
        while (tsdataIter.hasNext()) {
            tsdataIter.advance();
            final TimeSeriesCollection tsc = tsdataIter.value();

            for (SimpleGroupPath grp : tsc.getGroupPaths()) {
                final int grpRef = dict.getPathTable().getOrCreate(grp.getPath());
                if (!groupPathMap.containsKey(grpRef))
                    groupPathMap.put(grpRef, createTagTableArray(grp, ctx));
            }
        }

        // yield encoded table
        return new tables(Arrays.stream(groupPathMap.keys())
                .mapToObj(pathIdx -> {
                    tables_group tg = new tables_group();
                    tg.group_ref = pathIdx;
                    tg.tag_tbl = groupPathMap.get(pathIdx);
                    return tg;
                })
                .toArray(tables_group[]::new));
    }

    private tables_tag[] createTagTableArray(SimpleGroupPath grp, Context ctx) throws IOException, OncRpcException {
        TIntObjectMap<FilePos> tagMap = new TIntObjectHashMap<>();

        // Write dependency data.
        final TLongObjectIterator<TimeSeriesCollection> tsdataIter = tsdata.iterator();
        while (tsdataIter.hasNext()) {
            tsdataIter.advance();
            final TimeSeriesCollection tsc = tsdataIter.value();

            for (GroupName grpName : tsc.getGroups()) {
                if (Objects.equals(grpName.getPath(), grp)) {
                    final int tagRef = dict.getTagsTable().getOrCreate(grpName.getTags());
                    if (!tagMap.containsKey(tagRef))
                        tagMap.put(tagRef, writeGroupTable(grpName, ctx));
                }
            }
        }

        // yield encoded table
        return Arrays.stream(tagMap.keys())
                .mapToObj(tagIdx -> {
                    tables_tag tt = new tables_tag();
                    tt.tag_ref = tagIdx;
                    tt.pos = ToXdr.filePos(tagMap.get(tagIdx));
                    return tt;
                })
                .toArray(tables_tag[]::new);
    }

    private FilePos writeGroupTable(GroupName grpName, Context ctx) throws IOException, OncRpcException {
        TIntObjectMap<FilePos> metricsMap = new TIntObjectHashMap<>();
        TLongSet presence = new TLongHashSet();

        // Write dependency tables.
        final TLongObjectIterator<TimeSeriesCollection> tsdataIter = tsdata.iterator();
        while (tsdataIter.hasNext()) {
            tsdataIter.advance();
            final long ts = tsdataIter.key();
            final TimeSeriesCollection tsc = tsdataIter.value();

            final TimeSeriesValue tsv = tsc.get(grpName).orElse(null);
            if (tsv == null) continue;

            presence.add(ts);  // Record presence of this group.
            for (MetricName metricName : tsv.getMetrics().keySet()) {
                final int metricRef = dict.getPathTable().getOrCreate(metricName.getPath());
                if (!metricsMap.containsKey(metricRef))
                    metricsMap.put(metricRef, writeMetrics(grpName, metricName, ctx));
            }
        }

        // Write group table.
        group_table result = new group_table();
        result.presence = createPresenceBitset(presence, ctx.getTimestamps());
        result.metric_tbl = Arrays.stream(metricsMap.keys())
                .mapToObj(mIdx -> {
                    final tables_metric tm = new tables_metric();
                    tm.metric_ref = mIdx;
                    tm.pos = ToXdr.filePos(metricsMap.get(mIdx));
                    return tm;
                })
                .toArray(tables_metric[]::new);
        return ctx.newWriter().write(result);
    }

    private FilePos writeMetrics(GroupName grpName, MetricName metricName, Context ctx) throws IOException, OncRpcException {
        final MetricTable metricTbl = new MetricTable(dict);

        tsdata.forEachEntry((ts, tsc) -> {
            tsc.get(grpName)
                    .flatMap(tsv -> tsv.findMetric(metricName))
                    .ifPresent(value -> metricTbl.add(ts, value));

            return true;
        });

        return metricTbl.write(ctx.newWriter(), ctx.getTimestamps());
    }

    private tsfile_header encodeHeader(FilePos bodyPos, long fileSize, Context ctx) {
        tsfile_header hdr = new tsfile_header();
        hdr.first = ToXdr.timestamp(ctx.getBegin());
        hdr.last = ToXdr.timestamp(ctx.getEnd());
        hdr.flags = (ctx.isCompressed() ? header_flags.GZIP : 0) |
                header_flags.DISTINCT |
                header_flags.SORTED |
                header_flags.KIND_TABLES;
        hdr.reserved = 0;
        hdr.file_size = fileSize;
        hdr.fdt = ToXdr.filePos(bodyPos);
        return hdr;
    }

    private class Context {
        @Getter
        private final long timestamps[];
        @Getter
        private final ByteBuffer useBuffer;
        @Getter
        private final boolean compressed;
        @Getter
        private final FileChannelWriter fd;

        public Context(@NonNull FileChannel out, boolean compressed) {
            this.timestamps = tsdata.keys();
            Arrays.sort(this.timestamps);

            this.compressed = compressed;
            this.useBuffer = (compressed ? ByteBuffer.allocate(65536) : ByteBuffer.allocateDirect(65536));
            this.fd = new FileChannelWriter(out, HDR_SPACE);
        }

        public Writer newWriter() {
            return new Writer(fd, compressed, useBuffer);
        }

        public long getBegin() {
            return timestamps[0];
        }

        public long getEnd() {
            return timestamps[timestamps.length - 1];
        }
    }
}
