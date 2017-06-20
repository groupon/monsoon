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
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
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
import com.groupon.lex.metrics.history.xdr.support.Monitor;
import com.groupon.lex.metrics.history.xdr.support.TmpFileBasedColumnMajorTSData;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter.Writer;
import com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter;
import static com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter.CRC_LEN;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Stateful writer for table-based file.
 *
 * Table based files have to be written in one go, as they can't be written
 * before all data has been gathered.
 *
 * @author ariane
 */
public class ToXdrTables implements Closeable {
    private static final Logger LOG = Logger.getLogger(ToXdrTables.class.getName());
    private static final int HDR_SPACE = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
    private static final int MAX_BLOCK_RECORDS = 10000;

    private final TmpFileBasedColumnMajorTSData.Builder tsdataBuilder = TmpFileBasedColumnMajorTSData.builder();

    @Override
    public void close() throws IOException {
        /* SKIP */
    }

    public void add(@NonNull TimeSeriesCollection tsc) throws IOException {
        tsdataBuilder.with(tsc);
    }

    public void addAll(Collection<? extends TimeSeriesCollection> tscCollection) throws IOException {
        tsdataBuilder.with(tscCollection);
    }

    public DateTime build(FileChannel out, Compression compression) throws IOException {
        final CompletableFuture<tsfile_header> header;
        final DateTime tsBegin, tsEnd;

        try (final Context ctx = new Context(out, HDR_SPACE, compression)) {
            final TmpFileBasedColumnMajorTSData tsdata = tsdataBuilder.build();

            final List<TLongList> timestamps = partitionTimestamps(tsdata.getTimestamps().stream()
                    .mapToLong(DateTime::getMillis)
                    .distinct()
                    .sorted()
                    .collect(TLongArrayList::new, TLongArrayList::add, TLongArrayList::addAll));
            tsBegin = new DateTime(timestamps.get(0).get(0), DateTimeZone.UTC);
            tsEnd = new DateTime(timestamps.get(timestamps.size() - 1).get(timestamps.get(timestamps.size() - 1).size() - 1));

            final TLongIntMap lookupTable = buildTimestampLookupTable(timestamps);

            final List<BlockBuilder> blocks = timestamps.stream()
                    .map(timestampsPerBlock -> new BlockBuilder(timestampsPerBlock, ctx))
                    .collect(Collectors.toList());

            for (GroupName group : tsdata.getGroupNames()) {
                for (DateTime timestamp : tsdata.getGroupTimestamps(group)) {
                    final long ts = timestamp.getMillis();
                    final int blockIndex = lookupTable.get(ts);
                    if (blockIndex != lookupTable.getNoEntryValue())
                        blocks.get(blockIndex).accept(ts, group);
                }

                for (MetricName metric : tsdata.getMetricNames(group)) {
                    final TsvMetricBuilder[] tsvMetricBuilders = new TsvMetricBuilder[blocks.size()];

                    for (Map.Entry<DateTime, MetricValue> tsValue
                                 : tsdata.getMetricValues(group, metric).entrySet()) {
                        final long ts = tsValue.getKey().getMillis();
                        final int blockIndex = lookupTable.get(ts);
                        if (blockIndex == lookupTable.getNoEntryValue())
                            continue;

                        if (tsvMetricBuilders[blockIndex] == null)
                            tsvMetricBuilders[blockIndex] = blocks.get(blockIndex).newMetricBuilder(metric);
                        blocks.get(blockIndex).getTimestampIndex(ts)
                                .ifPresent(tsIndex -> tsvMetricBuilders[blockIndex].accept(tsIndex, tsValue.getValue()));
                    }

                    for (int i = 0; i < tsvMetricBuilders.length; ++i) {
                        if (tsvMetricBuilders[i] != null)
                            blocks.get(i).accept(group, metric, tsvMetricBuilders[i]);
                    }
                }
            }

            final CompletableFuture<file_data_tables> fdt = futuresToArray(blocks.stream().map(BlockBuilder::build).collect(Collectors.toList()), file_data_tables_block[]::new)
                    .thenApply(fdtBlocks -> {
                        file_data_tables result = new file_data_tables();
                        result.blocks = fdtBlocks;
                        return result;
                    });

            header = ctx.write(fdt)
                    .thenApply(fileDataPos -> encodeHeader(fileDataPos, ctx.getFd().getOffset(), true, true, compression, tsBegin, tsEnd));
        }

        try (XdrEncodingFileWriter xdr = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(out, 0), HDR_SPACE), 0), HDR_SPACE)) {
            xdr.beginEncoding();
            writeMimeHeader(xdr);
            deref(header).xdrEncode(xdr);
            xdr.endEncoding();
        } catch (OncRpcException ex) {
            throw new IOException("encoding error", ex);
        }

        return tsBegin;
    }

    @Value
    private static class BlockDerivedHeaderValues {
        private final boolean isOrdered;
        private final boolean isSorted;
    }

    private static class BlockBuilder {
        private final TLongList timestamps;
        private final TLongIntMap lookupTable;
        private final DictionaryForWrite dictionary = new DictionaryForWrite();
        private final Context ctx;
        private final TablesBuilder tablesBuilder;

        public BlockBuilder(@NonNull TLongList timestamps, @NonNull Context ctx) {
            this.timestamps = timestamps;
            this.lookupTable = buildTimestampLookupTable(this.timestamps);
            this.ctx = ctx;
            this.tablesBuilder = new TablesBuilder(this.ctx, this.timestamps.size());
        }

        public TsvMetricBuilder newMetricBuilder(MetricName metric) {
            return new TsvMetricBuilder(dictionary, ctx, timestamps.size(), dictionary.getPathTable().getOrCreate(metric.getPath()));
        }

        public OptionalInt getTimestampIndex(long ts) {
            int index = lookupTable.get(ts);
            return (index == lookupTable.getNoEntryValue() ? OptionalInt.empty() : OptionalInt.of(index));
        }

        public void accept(long ts, GroupName group) {
            getTimestampIndex(ts)
                    .ifPresent((index) -> tablesBuilder.accept(index, dictionary, group));
        }

        public void accept(GroupName group, MetricName metric, TsvMetricBuilder metricBuilder) {
            tablesBuilder.accept(dictionary, group, metric, metricBuilder);
        }

        public CompletableFuture<file_data_tables_block> build() {
            final CompletableFuture<FilePos> tables = ctx.write(tablesBuilder.build()); // Writes dependency data.
            final CompletableFuture<FilePos> encodedDictionary = ctx.write(dictionary.encode()); // Must be last.

            return tables.thenCombine(encodedDictionary, (FilePos tablesPos, FilePos encDictPos) -> {
                final file_data_tables_block result = new file_data_tables_block();
                result.tsd = ToXdr.timestamp_delta(timestamps.toArray());
                result.tables_data = ToXdr.filePos(tablesPos);
                result.dictionary = ToXdr.filePos(encDictPos);
                return result;
            });
        }

        private static TLongIntMap buildTimestampLookupTable(TLongList timestamps) {
            final TLongIntMap lookupTable = new TLongIntHashMap(timestamps.size(), 4, -1, -1);

            final TLongIterator iter = timestamps.iterator();
            for (int idx = 0; iter.hasNext(); ++idx)
                lookupTable.put(iter.next(), idx);

            return lookupTable;
        }
    }

    @RequiredArgsConstructor
    private static class TablesBuilder {
        @NonNull
        private final Context ctx;
        private final int timestampsSize;
        private final TIntObjectMap<GroupBuilder> groupBuilder = new TIntObjectHashMap<>();

        public void accept(int tsIndex, DictionaryForWrite dictionary, GroupName group) {
            final ResolvedGroup resolved = getGroupBuilderFor(dictionary, group);
            resolved.getBuilder().accept(tsIndex, dictionary, resolved.getPathIndex(), group.getTags());
        }

        public void accept(DictionaryForWrite dictionary, GroupName group, MetricName metric, TsvMetricBuilder metricBuilder) {
            final ResolvedGroup resolved = getGroupBuilderFor(dictionary, group);
            resolved.getBuilder().accept(dictionary, resolved.getPathIndex(), group.getTags(), metric, metricBuilder);
        }

        private ResolvedGroup getGroupBuilderFor(DictionaryForWrite dictionary, GroupName group) {
            final int pathIndex = dictionary.getPathTable().getOrCreate(group.getPath().getPath());

            GroupBuilder builder = groupBuilder.get(pathIndex);
            if (builder == null) {
                builder = new GroupBuilder(ctx, timestampsSize, pathIndex);
                groupBuilder.put(pathIndex, builder);
            }

            return new ResolvedGroup(pathIndex, builder);
        }

        public CompletableFuture<tables> build() {
            return futuresToArray(groupBuilder.valueCollection().stream().map(GroupBuilder::build).collect(Collectors.toList()), tables_group[]::new)
                    .thenApply(tables::new);
        }

        @RequiredArgsConstructor
        @Getter
        private static class ResolvedGroup {
            private final int pathIndex;
            private final GroupBuilder builder;
        }
    }

    @RequiredArgsConstructor
    private static class GroupBuilder {
        @NonNull
        private final Context ctx;
        private final int timestampsSize;
        private final TIntObjectMap<TagsBuilder> tsvBuilder = new TIntObjectHashMap<>();
        private final int groupRef;

        public void accept(int tsIndex, DictionaryForWrite dictionary, int groupIndex, Tags tags) {
            assert groupIndex == groupRef;
            final ResolvedTags resolved = getTsvBuilderFor(dictionary, tags);
            resolved.getBuilder().accept(tsIndex, dictionary, groupIndex, resolved.getTagIndex());
        }

        public void accept(DictionaryForWrite dictionary, int groupIndex, Tags tags, MetricName metric, TsvMetricBuilder metricBuilder) {
            assert groupIndex == groupRef;
            final ResolvedTags resolved = getTsvBuilderFor(dictionary, tags);
            resolved.getBuilder().accept(dictionary, groupIndex, resolved.getTagIndex(), metric, metricBuilder);
        }

        private ResolvedTags getTsvBuilderFor(DictionaryForWrite dictionary, Tags tags) {
            final int tagIndex = dictionary.getTagsTable().getOrCreate(tags);
            TagsBuilder builder = tsvBuilder.get(tagIndex);
            if (builder == null) {
                builder = new TagsBuilder(ctx, timestampsSize, tagIndex);
                tsvBuilder.put(tagIndex, builder);
            }
            return new ResolvedTags(tagIndex, builder);
        }

        public CompletableFuture<tables_group> build() {
            return futuresToArray(tsvBuilder.valueCollection().stream().map(TagsBuilder::build).collect(Collectors.toList()), tables_tag[]::new)
                    .thenApply(tagTables -> {
                        tables_group result = new tables_group();
                        result.group_ref = groupRef;
                        result.tag_tbl = tagTables;
                        return result;
                    });
        }

        @RequiredArgsConstructor
        @Getter
        private static class ResolvedTags {
            private final int tagIndex;
            private final TagsBuilder builder;
        }
    }

    private static class TagsBuilder {
        private final Context ctx;
        private final int tagsRef;
        private final TsvBuilder tsvBuilder;

        public TagsBuilder(@NonNull Context ctx, int timestampsSize, int tagsRef) {
            this.ctx = ctx;
            this.tagsRef = tagsRef;
            this.tsvBuilder = new TsvBuilder(timestampsSize);
        }

        public void accept(int tsIndex, DictionaryForWrite dictionary, int groupIndex, int tagsIndex) {
            assert tagsIndex == tagsRef;
            tsvBuilder.accept(tsIndex, dictionary);
        }

        public void accept(DictionaryForWrite dictionary, int groupIndex, int tagsIndex, MetricName metric, TsvMetricBuilder metricBuilder) {
            assert tagsIndex == tagsRef;
            tsvBuilder.accept(dictionary, metric, metricBuilder);
        }

        public CompletableFuture<tables_tag> build() {
            return ctx.write(tsvBuilder.build())
                    .thenApply(tsvPos -> {
                        final tables_tag result = new tables_tag();
                        result.tag_ref = tagsRef;
                        result.pos = ToXdr.filePos(tsvPos);
                        return result;
                    });
        }
    }

    private static class TsvBuilder {
        private final boolean[] presence;
        private final TIntObjectMap<CompletableFuture<tables_metric>> tsvMetricFutures = new TIntObjectHashMap<>();

        public TsvBuilder(int timestampsSize) {
            this.presence = new boolean[timestampsSize];
            Arrays.fill(this.presence, false);
        }

        public void accept(int tsIndex, DictionaryForWrite dictionary) {
            presence[tsIndex] = true;
        }

        public void accept(DictionaryForWrite dictionary, MetricName metric, TsvMetricBuilder metricBuilder) {
            final int metricIndex = dictionary.getPathTable().getOrCreate(metric.getPath());
            tsvMetricFutures.put(metricIndex, metricBuilder.build());
        }

        public CompletableFuture<group_table> build() {
            return futuresToArray(tsvMetricFutures.valueCollection(), tables_metric[]::new)
                    .thenApply(metrics -> {
                        group_table result = new group_table();
                        result.presence = ToXdr.bitset(presence);
                        result.metric_tbl = metrics;
                        return result;
                    });
        }

        @Value
        private static class ResolvedMetric {
            private final int metricRef;
            private final TsvMetricBuilder builder;
        }
    }

    private static class TsvMetricBuilder {
        private final DictionaryForWrite dictionary;
        private final Context ctx;
        private final int metricRef;
        private final MetricTable metricTable;

        public TsvMetricBuilder(@NonNull DictionaryForWrite dictionary, @NonNull Context ctx, int timestampsSize, int metricRef) {
            this.dictionary = dictionary;
            this.ctx = ctx;
            this.metricRef = metricRef;
            this.metricTable = new MetricTable(timestampsSize);
        }

        public void accept(int tsIndex, MetricValue value) {
            metricTable.add(tsIndex, dictionary, value);
        }

        public CompletableFuture<tables_metric> build() {
            return ctx.write(metricTable.encode())
                    .thenApply(mtPos -> {
                        tables_metric result = new tables_metric();
                        result.metric_ref = metricRef;
                        result.pos = ToXdr.filePos(mtPos);
                        return result;
                    });
        }
    }

    private static <T> CompletableFuture<T[]> futuresToArray(Collection<? extends CompletableFuture<? extends T>> futures, IntFunction<T[]> arrayConstructor) {
        if (futures.isEmpty())
            return CompletableFuture.completedFuture(arrayConstructor.apply(0));
        final T[] result = arrayConstructor.apply(futures.size());

        final CompletableFuture[] combination = new CompletableFuture[futures.size()];
        final Iterator<? extends CompletableFuture<? extends T>> futuresIter = futures.iterator();
        for (int idx = 0; idx < futures.size(); ++idx) {
            final int resultIndex = idx;
            combination[idx] = (futuresIter.next().thenAccept((T v) -> result[resultIndex] = v));
        }
        assert !futuresIter.hasNext();

        return CompletableFuture.allOf(combination)
                .thenApply((voidValue) -> result);
    }

    /**
     * Partition timestamps according to the blocks that they should be in.
     *
     * @param timestamps The complete set of timestamps. Must be unique and
     * ordered.
     * @return A list of timestamp partitions.
     */
    private static List<TLongList> partitionTimestamps(TLongList timestamps) {
        if (timestamps.isEmpty()) return emptyList();
        final List<TLongList> partitions = new ArrayList<>();

        TLongList active = new TLongArrayList();
        final TLongIterator tsIter = timestamps.iterator();
        active.add(tsIter.next());

        while (tsIter.hasNext()) {
            final long next = tsIter.next();
            assert next > active.get(active.size() - 1);

            if (next - active.get(active.size() - 1) > Integer.MAX_VALUE || active.size() >= MAX_BLOCK_RECORDS) {
                partitions.add(active);
                active = new TLongArrayList();
            }
            active.add(next);
        }
        partitions.add(active);

        assert partitions.stream().mapToInt(TLongList::size).sum() == timestamps.size();
        return partitions;
    }

    /**
     * Build a lookup table that maps timestamps to their block index.
     *
     * @param partitions The timestamp partition table.
     * @return A lookup table, with the keys being any of the timestamps,
     * mapping to the index in the partitions list.
     */
    private static TLongIntMap buildTimestampLookupTable(List<TLongList> partitions) {
        final TLongIntMap lookupTable = new TLongIntHashMap(100, 4, -1, -1);

        final ListIterator<TLongList> iter = partitions.listIterator();
        while (iter.hasNext()) {
            final int partitionIndex = iter.nextIndex();
            iter.next().forEach((ts) -> {
                lookupTable.put(ts, partitionIndex);
                return true;
            });
        }

        return lookupTable;
    }

    private tsfile_header encodeHeader(FilePos bodyPos, long fileSize, boolean blocksAreOrdered, boolean blocksAreDistinct, Compression compression, DateTime hdrBegin, DateTime hdrEnd) {
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

    private static class Context implements AutoCloseable {
        @Getter
        private final ByteBuffer useBuffer;
        @Getter
        private final FileChannelWriter fd;
        private final Monitor<XdrAble, FilePos> writer;

        public Context(FileChannel out, long fileOffset, Compression compression) {
            this.useBuffer = (compression == Compression.NONE ? ByteBuffer.allocate(65536) : ByteBuffer.allocateDirect(65536));
            this.fd = new FileChannelWriter(out, fileOffset);
            this.writer = new Monitor<>(new Writer(fd, compression, useBuffer, true)::write);
        }

        public CompletableFuture<FilePos> write(XdrAble data) {
            return writer.enqueue(data);
        }

        public CompletableFuture<FilePos> write(CompletableFuture<? extends XdrAble> futureData) {
            return writer.enqueueFuture(futureData);
        }

        @Override
        public void close() {
            writer.close();
        }
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
            } catch (ExecutionException ex) {
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
