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
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.ToXdr.createPresenceBitset;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables_block;
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
import com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter;
import static com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter.CRC_LEN;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 * Stateful writer for table-based file.
 *
 * Table based files have to be written in one go, as they can't be written
 * before all data has been gathered.
 *
 * @author ariane
 */
public class ToXdrTables implements Closeable {
    private static final long HDR_SPACE = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
    private static final int MAX_BLOCK_RECORDS = 10000;
    private final Queue<TimeSeriesCollection> tsdataQueue = new PriorityQueue<>();

    public void addAll(Collection<? extends TimeSeriesCollection> tsc) {
        tsdataQueue.addAll(tsc);
    }

    public void write(@NonNull FileChannel out, Compression compression) throws OncRpcException, IOException {
        writeFile(out, compression);
    }

    public void write(@NonNull FileChannel out) throws OncRpcException, IOException {
        writeFile(out, Compression.DEFAULT);
    }

    @Override
    public void close() throws IOException {
    }

    private void writeFile(FileChannel out, Compression compression) throws IOException, OncRpcException {
        if (tsdataQueue.isEmpty())
            throw new IllegalStateException("table files may not be empty");
        final Context ctx = new Context(out, compression);

        final FilePos bodyPos;
        final long fileEnd;
        try (FileChannelWriter fd = new FileChannelWriter(out, HDR_SPACE)) {
            bodyPos = writeBody(ctx);
            fileEnd = fd.getOffset();
        }

        try (XdrEncodingFileWriter xdr = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(out, 0), HDR_SPACE), 0), ctx.getUseBuffer())) {
            xdr.beginEncoding();
            writeMimeHeader(xdr);
            encodeHeader(bodyPos, fileEnd, ctx).xdrEncode(xdr);
            xdr.endEncoding();
        }
    }

    private FilePos writeBody(Context ctx) throws IOException, OncRpcException {
        file_data_tables result = new file_data_tables();

        final List<file_data_tables_block> blocks = new ArrayList<>();
        while (!tsdataQueue.isEmpty())
            blocks.add(createBlock(ctx));
        result.blocks = blocks.toArray(new file_data_tables_block[blocks.size()]);

        return ctx.newWriter().write(result);
    }

    private List<TimeSeriesCollection> selectTsdataForBlock() {
        ArrayList<TimeSeriesCollection> tsdata = new ArrayList<>(Integer.min(tsdataQueue.size(), MAX_BLOCK_RECORDS));
        DateTime recentTs = tsdataQueue.element().getTimestamp().toDateTime(DateTimeZone.UTC);

        while (tsdata.size() < MAX_BLOCK_RECORDS) {
            final TimeSeriesCollection tsc = tsdataQueue.peek();
            if (tsc == null)
                break;

            Duration tscDelta = new Duration(recentTs, tsc.getTimestamp());
            if (tscDelta.getMillis() > Integer.MAX_VALUE)
                break;
            recentTs = tsc.getTimestamp().toDateTime(DateTimeZone.UTC);
            tsdata.add(tsc);

            TimeSeriesCollection removed = tsdataQueue.remove();
            assert (tsc == removed);
        }
        tsdata.trimToSize();
        return tsdata;
    }

    private file_data_tables_block createBlock(Context ctx) throws IOException, OncRpcException {
        ctx.setTsdata(selectTsdataForBlock());
        ctx.setDict(new DictionaryForWrite());

        file_data_tables_block result = new file_data_tables_block();
        try {
            result.tables_data = ToXdr.filePos(ctx.newWriter().write(createTables(ctx)));  // Writes dependency data.
            result.dictionary = ToXdr.filePos(ctx.newWriter().write(ctx.getDict().encode()));  // Must be last.
            result.tsd = ToXdr.timestamp_delta(ctx.getTimestamps());
        } finally {
            ctx.setTsdata(null);
            ctx.setDict(null);
        }

        return result;
    }

    private Map<SimpleGroupPath, Set<GroupName>> computeGroupKeys(Context ctx) {
        return ctx.getTsdata().stream()
                .flatMap(tsc -> tsc.getGroups().stream())
                .collect(Collectors.groupingBy(GroupName::getPath, Collectors.toSet()));
    }

    private tables createTables(Context ctx) throws IOException, OncRpcException {
        TIntObjectMap<tables_tag[]> groupPathMap = new TIntObjectHashMap<>();

        // Write dependency data.
        for (Map.Entry<SimpleGroupPath, Set<GroupName>> grpEntry : computeGroupKeys(ctx).entrySet()) {
            final SimpleGroupPath grp = grpEntry.getKey();
            final int grpRef = ctx.getDict().getPathTable().getOrCreate(grp.getPath());
            assert (!groupPathMap.containsKey(grpRef));
            groupPathMap.put(grpRef, createTagTableArray(grp, grpEntry.getValue(), ctx));
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

    private tables_tag[] createTagTableArray(SimpleGroupPath grp, Collection<GroupName> grpNames, Context ctx) throws IOException, OncRpcException {
        TIntObjectMap<FilePos> tagMap = new TIntObjectHashMap<>();

        // Write dependency data.
        for (GroupName grpName : grpNames) {
            assert (Objects.equals(grp, grpName.getPath()));
            final int tagRef = ctx.getDict().getTagsTable().getOrCreate(grpName.getTags());
            assert (!tagMap.containsKey(tagRef));
            tagMap.put(tagRef, writeGroupTable(grpName, ctx));
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

    private Set<MetricName> computeMetricNames(Context ctx, GroupName grpName) {
        return ctx.getTsdata().stream()
                .map(tsc -> tsc.get(grpName))
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .flatMap(tsv -> tsv.getMetrics().keySet().stream())
                .collect(Collectors.toSet());
    }

    private FilePos writeGroupTable(GroupName grpName, Context ctx) throws IOException, OncRpcException {
        TIntObjectMap<FilePos> metricsMap = new TIntObjectHashMap<>();
        TLongSet presence = new TLongHashSet();

        // Fill presence set.
        ctx.getTsdata().stream()
                .filter(tsv -> tsv.get(grpName).isPresent())
                .mapToLong(tsv -> tsv.getTimestamp().toDateTime(DateTimeZone.UTC).getMillis())
                .forEach(presence::add);

        // Write dependency tables.
        for (MetricName metricName : computeMetricNames(ctx, grpName)) {
            final int metricRef = ctx.getDict().getPathTable().getOrCreate(metricName.getPath());
            assert (!metricsMap.containsKey(metricRef));
            metricsMap.put(metricRef, writeMetrics(grpName, metricName, ctx));
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
        final MetricTable metricTbl = new MetricTable(ctx.getDict());

        ctx.getTsdata().forEach(tsc -> {
            tsc.get(grpName)
                    .flatMap(tsv -> tsv.findMetric(metricName))
                    .ifPresent(value -> metricTbl.add(tsc.getTimestamp().toDateTime(DateTimeZone.UTC).getMillis(), value));
        });

        return metricTbl.write(ctx.newWriter(), ctx.getTimestamps());
    }

    private tsfile_header encodeHeader(FilePos bodyPos, long fileSize, Context ctx) {
        tsfile_header hdr = new tsfile_header();
        hdr.first = ToXdr.timestamp(ctx.getBegin());
        hdr.last = ToXdr.timestamp(ctx.getEnd());
        hdr.flags = ctx.getCompression().compressionFlag
                | header_flags.DISTINCT
                | header_flags.SORTED
                | header_flags.KIND_TABLES;
        hdr.reserved = 0;
        hdr.file_size = fileSize;
        hdr.fdt = ToXdr.filePos(bodyPos);
        return hdr;
    }

    private class Context {
        @Getter
        private List<TimeSeriesCollection> tsdata = null;
        @Getter
        private long timestamps[] = null;
        @Getter
        private final ByteBuffer useBuffer;
        @Getter
        private final Compression compression;
        @Getter
        private final FileChannelWriter fd;
        @Getter
        @Setter
        private DictionaryForWrite dict;
        private Long begin = null, end = null;

        public Context(@NonNull FileChannel out, Compression compression) {
            this.compression = compression;
            this.useBuffer = (compression == Compression.NONE ? ByteBuffer.allocate(65536) : ByteBuffer.allocateDirect(65536));
            this.fd = new FileChannelWriter(out, HDR_SPACE);
        }

        public void setTsdata(List<TimeSeriesCollection> tsdata) {
            this.tsdata = tsdata;
            if (tsdata != null) {
                this.timestamps = tsdata.stream()
                        .mapToLong(tsc -> tsc.getTimestamp().toDateTime(DateTimeZone.UTC).getMillis())
                        .sorted()
                        .toArray();
                Arrays.sort(this.timestamps);

                if (begin == null || timestamps[0] < begin)
                    begin = timestamps[0];
                if (end == null || timestamps[timestamps.length - 1] > end)
                    end = timestamps[timestamps.length - 1];
            } else {
                this.timestamps = null;
            }
        }

        public Writer newWriter() {
            return new Writer(fd, compression, useBuffer);
        }

        public long getBegin() {
            return begin;
        }

        public long getEnd() {
            return end;
        }
    }
}
