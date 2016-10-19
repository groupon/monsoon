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
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.ToXdr.createPresenceBitset;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables_block;
import com.groupon.lex.metrics.history.v2.xdr.group_table;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.v2.xdr.tables;
import com.groupon.lex.metrics.history.v2.xdr.tables_group;
import com.groupon.lex.metrics.history.v2.xdr.tables_metric;
import com.groupon.lex.metrics.history.v2.xdr.tables_tag;
import com.groupon.lex.metrics.history.v2.xdr.timestamp_msec;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.Const.writeMimeHeader;
import com.groupon.lex.metrics.history.xdr.support.FJPTaskExecutor;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.Monitor;
import com.groupon.lex.metrics.history.xdr.support.TmpFile;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter.Writer;
import com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter;
import static com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter.CRC_LEN;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;
import org.acplt.oncrpc.XdrEncodingStream;
import org.joda.time.DateTimeZone;

/**
 * Stateful writer for table-based file.
 *
 * Table based files have to be written in one go, as they can't be written
 * before all data has been gathered.
 *
 * @author ariane
 */
@RequiredArgsConstructor
public class ToXdrTables implements Closeable {
    private static final Logger LOG = Logger.getLogger(ToXdrTables.class.getName());
    private static final int HDR_SPACE = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
    private static final int MAX_BLOCK_RECORDS = 10000;
    private static final Compression TMPFILE_COMPRESSION = Compression.SNAPPY;
    @NonNull
    private final FileChannel out;
    @NonNull
    private final Compression compression;

    private final TLongSet timestamps = new TLongHashSet();
    private final ConcurrentMap<GroupName, GroupTmpFile> groups = new ConcurrentHashMap<>(1000, 0.5f, FJPTaskExecutor.DEFAULT_CONCURRENCY);
    private DictionaryForWrite dictionary;
    private final List<file_data_tables_block> blocks = new ArrayList<>();
    private Long firstTs = null, lastTs = null;  // Timestamps in current block.
    private long fileOffset = HDR_SPACE;
    @Getter
    private Long hdrBegin, hdrEnd;  // Timestamps for the entire file.

    public void add(@NonNull TimeSeriesCollection tsc) throws IOException {
        final long ts0 = System.currentTimeMillis();
        try {
            final long timestamp = tsc.getTimestamp().toDateTime(DateTimeZone.UTC).getMillis();
            if (hdrBegin == null)
                hdrBegin = timestamp;
            else
                hdrBegin = Long.min(hdrBegin, timestamp);
            if (hdrEnd == null)
                hdrEnd = timestamp;
            else
                hdrEnd = Long.max(hdrEnd, timestamp);

            try {
                // Decide if we should flush out the current block.
                if (firstTs != null && lastTs != null) {
                    assert !timestamps.isEmpty();
                    boolean flushNow = timestamps.size() >= MAX_BLOCK_RECORDS;
                    if (timestamp > lastTs && timestamp - lastTs > Integer.MAX_VALUE)
                        flushNow = true;
                    if (timestamp < firstTs && firstTs - timestamp > Integer.MAX_VALUE)
                        flushNow = true;
                    if (flushNow) {
                        LOG.log(Level.INFO, "creating block {0}", blocks.size());
                        blocks.add(createBlock(new Context()));
                    }
                }
                // Update firstTs and lastTs for current block.
                if (firstTs != null) {  // Existing block.
                    assert lastTs != null;
                    assert !timestamps.isEmpty();
                    assert dictionary != null;
                    firstTs = Long.min(firstTs, timestamp);
                    lastTs = Long.max(lastTs, timestamp);
                } else {  // First record on new block.
                    assert timestamps.isEmpty();
                    assert lastTs == null;
                    assert dictionary == null;
                    firstTs = lastTs = timestamp;
                    dictionary = new DictionaryForWrite();
                }

                timestamps.add(timestamp);
                new FJPTaskExecutor<>(tsc.getTSValues(), tsv -> processGroup(timestamp, tsv))
                        .join();
            } catch (OncRpcException ex) {
                throw new IOException("encoding error", ex);
            }
        } finally {
            LOG.log(Level.FINE, "adding timeseries {0} took {1} msec", new Object[]{tsc.getTimestamp(), System.currentTimeMillis() - ts0});
        }
    }

    private void processGroup(long timestamp, @NonNull TimeSeriesValue tsv) throws IOException, OncRpcException {
        final GroupTmpFile group = groups.computeIfAbsent(
                tsv.getGroup(),
                grp -> {
                    try {
                        return new GroupTmpFile(dictionary);
                    } catch (IOException ex) {
                        throw new RuntimeException("unable to create temporary file for " + grp, ex);
                    }
                });
        group.add(timestamp, tsv.getMetrics());
    }

    public void addAll(Collection<? extends TimeSeriesCollection> tscCollection) throws IOException {
        for (TimeSeriesCollection tsc : tscCollection)
            add(tsc);
    }

    public void write() throws OncRpcException, IOException {
        writeFile();
    }

    @Override
    public void close() throws IOException {
        try {
            while (!timestamps.isEmpty())
                blocks.add(createBlock(new Context()));
            writeFile();
        } catch (OncRpcException ex) {
            throw new IOException("encoding error", ex);
        } finally {
            closeGroups();
        }
    }

    public void closeGroups() throws IOException {
        IOException exception = null;
        try {
            for (Closeable group : groups.values()) {
                try {
                    group.close();
                } catch (IOException ex) {
                    if (exception == null)
                        exception = ex;
                    else
                        exception.addSuppressed(ex);
                }
            }
        } finally {
            groups.clear();
        }
        if (exception != null)
            throw exception;
    }

    private void writeFile() throws IOException, OncRpcException {
        if (blocks.isEmpty())
            throw new IllegalStateException("table files may not be empty");
        Collections.sort(blocks, Comparator.comparing(block -> block.tsd.first));

        final FilePos bodyPos;
        final long fileEnd;
        {
            final Context ctx = new Context();
            bodyPos = writeBody(ctx);
            fileEnd = fileOffset = ctx.getFd().getOffset();
        }

        try (XdrEncodingFileWriter xdr = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(out, 0), HDR_SPACE), 0), HDR_SPACE)) {
            xdr.beginEncoding();
            writeMimeHeader(xdr);
            final boolean blocksAreOrdered = blocksAreOrdered(blocks);
            final boolean blocksAreDistinct = blocksAreOrdered || blocksAreDistinct(blocks);
            encodeHeader(bodyPos, fileEnd, blocksAreOrdered, blocksAreDistinct).xdrEncode(xdr);
            xdr.endEncoding();
        }
    }

    private static boolean blocksAreOrdered(List<file_data_tables_block> blocks) {
        if (blocks.size() <= 1)
            return true;
        final Iterator<file_data_tables_block> iter = blocks.iterator();

        // Initialization: figure out end of first block.
        long predecessorTs;  // Invariant: last timestamp (long) of block preceding iter.next().
        {
            long[] predecessorTsd = FromXdr.timestamp_delta(iter.next().tsd);
            predecessorTs = predecessorTsd[predecessorTsd.length - 1];
        }

        while (iter.hasNext()) {
            final file_data_tables_block current = iter.next();
            if (current.tsd.first <= predecessorTs)  // Intersection: blocks are not ordered.
                return false;

            long[] currentTsd = FromXdr.timestamp_delta(current.tsd);
            predecessorTs = currentTsd[currentTsd.length - 1];
        }
        return true;
    }

    private static boolean blocksAreDistinct(List<file_data_tables_block> blocks) {
        if (blocks.size() <= 1)
            return true;
        final TLongIntHashMap tsWithCount = blocks.parallelStream()
                .flatMapToLong(block -> Arrays.stream(FromXdr.timestamp_delta(block.tsd)))
                .collect(
                        TLongIntHashMap::new,
                        (map, value) -> map.adjustOrPutValue(value, 1, 1),
                        (result, add) -> {
                            add.forEachEntry((ts, count) -> {
                                result.adjustOrPutValue(ts, count, count);
                                return true;
                            });
                        });

        final TIntIterator countIter = tsWithCount.valueCollection().iterator();
        while (countIter.hasNext())
            if (countIter.next() > 1)
                return false;
        return true;
    }

    private FilePos writeBody(Context ctx) throws IOException, OncRpcException {
        file_data_tables result = new file_data_tables();

        if (blocks.isEmpty())
            throw new IOException("table file may not be empty");
        result.blocks = blocks.toArray(new file_data_tables_block[blocks.size()]);

        return deref(ctx.write(result));
    }

    private file_data_tables_block createBlock(Context ctx) throws IOException, OncRpcException {
        ctx.setTsdata(timestamps);
        Future<FilePos> tables = ctx.write(createTables(ctx));  // Writes dependency data.
        Future<FilePos> encodedDictionary = ctx.write(dictionary.encode());  // Must be last.

        file_data_tables_block result = new file_data_tables_block();
        result.tables_data = ToXdr.filePos(deref(tables));
        result.dictionary = ToXdr.filePos(deref(encodedDictionary));
        result.tsd = ToXdr.timestamp_delta(ctx.getTimestamps());

        // Reset state.
        timestamps.clear();
        dictionary = null;
        try {
            closeGroups();
        } catch (Exception ex) {
            LOG.log(Level.WARNING, "unable to close temporary files properly", ex);
        }
        firstTs = lastTs = null;
        fileOffset = ctx.getFd().getOffset();  // Update file offset after successful write.

        return result;
    }

    private tables createTables(Context ctx) throws IOException, OncRpcException {
        TIntObjectMap<List<tables_tag>> groupPathMap = new TIntObjectHashMap<>();

        // Write dependency data.
        new FJPTaskExecutor<>(
                groups.entrySet(),
                grpEntry -> {
                    final SimpleGroupPath grp = grpEntry.getKey().getPath();
                    final int grpRef = dictionary.getPathTable().getOrCreate(grp.getPath());
                    final List<tables_tag> tagList;
                    synchronized (groupPathMap) {
                        if (!groupPathMap.containsKey(grpRef))
                            groupPathMap.put(grpRef, new ArrayList<>());
                        tagList = groupPathMap.get(grpRef);
                    }

                    final tables_tag tables_tag = createTagTable(grpEntry.getKey(), grpEntry.getValue(), ctx);
                    synchronized (tagList) {
                        tagList.add(tables_tag);
                    }
                })
                .join();

        // yield encoded table
        return new tables(Arrays.stream(groupPathMap.keys())
                .mapToObj(pathIdx -> {
                    tables_group tg = new tables_group();
                    tg.group_ref = pathIdx;
                    tg.tag_tbl = groupPathMap.get(pathIdx).toArray(new tables_tag[0]);
                    assert tg.tag_tbl.length > 0;
                    return tg;
                })
                .toArray(tables_group[]::new));
    }

    private tables_tag createTagTable(GroupName grpName, GroupTmpFile grpData, Context ctx) throws IOException, OncRpcException {
        // Write dependency data.
        final int tagRef = dictionary.getTagsTable().getOrCreate(grpName.getTags());
        final FilePos groupPos = writeGroupTable(grpData, ctx);

        // yield encoded table
        tables_tag tt = new tables_tag();
        tt.tag_ref = tagRef;
        tt.pos = ToXdr.filePos(groupPos);
        return tt;
    }

    private FilePos writeGroupTable(GroupTmpFile grpData, Context ctx) throws IOException, OncRpcException {
        TLongSet presence = new TLongHashSet();

        grpData.iterator().forEachRemaining(presence::add);
        TIntObjectMap<FilePos> metricsMap = writeMetrics(grpData.getMetricData(), ctx);  // Write dependency tables.

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
        return deref(ctx.write(result));
    }

    private TIntObjectMap<FilePos> writeMetrics(MetricTmpFile metrics, Context ctx) throws IOException, OncRpcException {
        final TIntObjectMap<Future<FilePos>> futurePos = new TIntObjectHashMap<>();
        {
            final TIntObjectMap<MetricTable> metricTbl = new TIntObjectHashMap<>();
            metrics.iterator().forEachRemaining(timestampedMetric -> {
                final int nameRef = timestampedMetric.getName();
                if (!metricTbl.containsKey(nameRef))
                    metricTbl.put(nameRef, new MetricTable(dictionary));
                metricTbl.get(nameRef).add(timestampedMetric.getTimestamp(), timestampedMetric.getValue());
            });

            metricTbl.forEachEntry((nameRef, mt) -> {
                futurePos.put(nameRef, ctx.write(mt.encode(ctx.getTimestamps())));
                return true;
            });
        }

        final TIntObjectMap<FilePos> result = new TIntObjectHashMap<>();
        for (int nameRef : futurePos.keys())
            result.put(nameRef, deref(futurePos.get(nameRef)));
        return result;
    }

    private tsfile_header encodeHeader(FilePos bodyPos, long fileSize, boolean blocksAreOrdered, boolean blocksAreDistinct) {
        tsfile_header hdr = new tsfile_header();
        hdr.first = ToXdr.timestamp(hdrBegin);
        hdr.last = ToXdr.timestamp(hdrEnd);
        hdr.flags = compression.compressionFlag
                | (blocksAreDistinct ? header_flags.DISTINCT : 0)
                | (blocksAreOrdered ? header_flags.SORTED : 0)
                | header_flags.KIND_TABLES;
        hdr.reserved = 0;
        hdr.file_size = fileSize;
        hdr.fdt = ToXdr.filePos(bodyPos);
        return hdr;
    }

    private class Context {
        @Getter
        private long timestamps[] = null;
        @Getter
        private final ByteBuffer useBuffer;
        @Getter
        private final FileChannelWriter fd;
        private Long begin = null, end = null;
        private final Monitor<XdrAble, FilePos> writer;

        public Context() {
            this.useBuffer = (compression == Compression.NONE ? ByteBuffer.allocate(65536) : ByteBuffer.allocateDirect(65536));
            this.fd = new FileChannelWriter(out, fileOffset);
            this.writer = new Monitor<>(new Writer(fd, compression, useBuffer, true)::write);
        }

        public void setTsdata(TLongSet tsdata) {
            if (tsdata != null) {
                this.timestamps = tsdata.toArray();
                Arrays.sort(this.timestamps);

                if (begin == null || timestamps[0] < begin)
                    begin = timestamps[0];
                if (end == null || timestamps[timestamps.length - 1] > end)
                    end = timestamps[timestamps.length - 1];
            } else {
                this.timestamps = null;
            }
        }

        public Future<FilePos> write(XdrAble data) {
            return writer.enqueue(data);
        }

        public long getBegin() {
            return begin;
        }

        public long getEnd() {
            return end;
        }
    }

    private static class GroupTmpFile implements Closeable {
        private final TmpFile<timestamp_msec> groupData;
        @Getter
        private final MetricTmpFile metricData;

        public GroupTmpFile(@NonNull DictionaryForWrite dictionary) throws IOException {
            this.groupData = new TmpFile<>(TMPFILE_COMPRESSION);
            this.metricData = new MetricTmpFile(dictionary);
        }

        public GroupTmpFile(@NonNull Path dir, @NonNull DictionaryForWrite dictionary) throws IOException {
            this.groupData = new TmpFile<>(dir, TMPFILE_COMPRESSION);
            this.metricData = new MetricTmpFile(dir, dictionary);
        }

        public void add(long timestamp, @NonNull Map<MetricName, MetricValue> metrics) throws IOException, OncRpcException {
            groupData.add(ToXdr.timestamp(timestamp));
            for (Map.Entry<MetricName, MetricValue> metric : metrics.entrySet())
                metricData.add(timestamp, metric.getKey(), metric.getValue());
        }

        public Iterator<Long> iterator() throws IOException, OncRpcException {
            final Iterator<timestamp_msec> underlying = groupData.iterator(timestamp_msec::new);
            return new Iterator<Long>() {
                @Override
                public boolean hasNext() {
                    return underlying.hasNext();
                }

                @Override
                public Long next() {
                    return FromXdr.timestamp(underlying.next()).getMillis();
                }
            };
        }

        @Override
        public void close() throws IOException {
            IOException exception = null;
            try {
                groupData.close();
            } catch (IOException ex) {
                exception = ex;
            }

            try {
                metricData.close();
            } catch (IOException ex) {
                if (exception != null)
                    exception.addSuppressed(ex);
                else
                    exception = ex;
            }

            if (exception != null)
                throw exception;
        }
    }

    private static class MetricTmpFile implements Closeable {
        private final TmpFile<Atom> metricData;
        private final DictionaryForWrite dictionary;

        public MetricTmpFile(@NonNull DictionaryForWrite dictionary) throws IOException {
            this.metricData = new TmpFile<>(TMPFILE_COMPRESSION);
            this.dictionary = dictionary;
        }

        public MetricTmpFile(@NonNull Path dir, @NonNull DictionaryForWrite dictionary) throws IOException {
            metricData = new TmpFile<>(dir, TMPFILE_COMPRESSION);
            this.dictionary = dictionary;
        }

        public void add(long timestamp, @NonNull MetricName mn, @NonNull MetricValue mv) throws IOException, OncRpcException {
            metricData.add(new Atom(timestamp, mn, mv, dictionary));
        }

        public Iterator<MetricRecord> iterator() throws IOException, OncRpcException {
            Iterator<Atom> underlying = metricData.iterator(() -> new Atom());
            return new Iterator<MetricRecord>() {
                @Override
                public boolean hasNext() {
                    return underlying.hasNext();
                }

                @Override
                public MetricRecord next() {
                    Atom next = underlying.next();
                    return new MetricRecord(next.getTimestamp(), next.getMetricName(), next.getMetricValue());
                }
            };
        }

        @Override
        public void close() throws IOException {
            metricData.close();
        }

        @Getter
        @NoArgsConstructor
        private static class Atom implements XdrAble {
            private long timestamp;
            private int metricName;
            private metric_value metricValue;

            public Atom(long timestamp, @NonNull MetricName metricName, @NonNull MetricValue metricValue, @NonNull DictionaryForWrite dictionary) {
                this.timestamp = timestamp;
                this.metricName = dictionary.getPathTable().getOrCreate(metricName.getPath());
                this.metricValue = ToXdr.metricValue(metricValue, dictionary.getStringTable()::getOrCreate);
            }

            @Override
            public void xdrEncode(@NonNull XdrEncodingStream stream) throws OncRpcException, IOException {
                ToXdr.timestamp(timestamp).xdrEncode(stream);
                stream.xdrEncodeInt(metricName);
                metricValue.xdrEncode(stream);
            }

            @Override
            public void xdrDecode(@NonNull XdrDecodingStream stream) throws OncRpcException, IOException {
                timestamp = FromXdr.timestamp(new timestamp_msec(stream)).getMillis();
                metricName = stream.xdrDecodeInt();
                metricValue = new metric_value(stream);
            }
        }
    }

    @Value
    private static class MetricRecord {
        private final long timestamp;
        private final int name;
        private final metric_value value;
    }

    /**
     * Dereference a future and if it failed, unwrap the exception.
     */
    private static <T> T deref(Future<T> future) throws IOException, OncRpcException {
        for (;;) {
            try {
                return future.get();
            } catch (InterruptedException ex) {
                LOG.log(Level.WARNING, "interrupted while waiting for future", ex);
            } catch (ExecutionException | Monitor.MonitorException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof Error)
                    throw (Error) cause;
                if (cause instanceof RuntimeException)
                    throw (RuntimeException) cause;
                if (cause instanceof IOException)
                    throw (IOException) cause;
                if (cause instanceof OncRpcException)
                    throw (OncRpcException) cause;
                throw new IllegalStateException("unexpected exception type", ex);
            }
        }
    }
}
