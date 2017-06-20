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
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.ColumnMajorTSData;
import com.groupon.lex.metrics.history.xdr.Const;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrow;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.SequenceTSData;
import com.groupon.lex.metrics.history.xdr.support.reader.Crc32VerifyingFileReader;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelSegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import gnu.trove.list.TLongList;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Getter;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class ReadonlyTableFile extends SequenceTSData implements ColumnMajorTSData {
    private static final short FILE_VERSION = 2;  // Only file version that uses Table format.
    private final SegmentReader<RTFFileDataTables> body;
    private final GCCloseable<FileChannel> fd;
    @Getter
    private final DateTime begin, end;
    @Getter
    private final long fileSize;
    private final int version;
    private final SegmentReader.Factory<XdrAble> segmentFactory;

    @Override
    public boolean add(TimeSeriesCollection tsv) {
        throw new UnsupportedOperationException("add");
    }

    @Override
    public ObjectSequence<TimeSeriesCollection> getSequence() {
        return body.decodeOrThrow().getSequence().decodeOrThrow();
    }

    @Override
    public short getMajor() {
        return Const.version_major(version);
    }

    @Override
    public short getMinor() {
        return Const.version_minor(version);
    }

    @Override
    public boolean canAddSingleRecord() {
        return false;
    }

    @Override
    public boolean isOptimized() {
        return true;
    }

    @Override
    public Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.of(fd);
    }

    public ReadonlyTableFile(GCCloseable<FileChannel> file) throws IOException, OncRpcException {
        fileSize = file.get().size();
        fd = file;
        final tsfile_header hdr;

        /* Nest readers to handle CRC verification of the header. */
        try (XdrDecodingFileReader reader = new XdrDecodingFileReader(new Crc32VerifyingFileReader(new FileChannelReader(file.get(), 0), MIME_HEADER_LEN + HDR_3_LEN, 0))) {
            reader.beginDecoding();

            /* Check the mime header and version number first. */
            version = validateHeaderOrThrow(reader);
            if (Const.version_major(version) != FILE_VERSION)
                throw new IllegalArgumentException("TableFile is version " + FILE_VERSION + " only");

            /* Read the header; we don't actually process its information until
             * the CRC validation completes (it's in the close part of try-with-resources). */
            hdr = new tsfile_header(reader);

            reader.endDecoding();
        } // CRC is validated here.

        if (hdr.file_size > fileSize)
            throw new IOException("file truncated");
        if ((hdr.flags & header_flags.KIND_MASK) != header_flags.KIND_TABLES)
            throw new IllegalArgumentException("Not a file in table encoding");

        final Compression compression = Compression.fromFlags(hdr.flags);
        final boolean distinct = ((hdr.flags & header_flags.DISTINCT) == header_flags.DISTINCT);
        final boolean sorted = ((hdr.flags & header_flags.SORTED) == header_flags.SORTED);
        begin = FromXdr.timestamp(hdr.first);
        end = FromXdr.timestamp(hdr.last);

        segmentFactory = new FileChannelSegmentReader.Factory(file, compression);
        final FilePos bodyPos = FromXdr.filePos(hdr.fdt);

        body = segmentFactory.get(file_data_tables::new, bodyPos)
                .map(fdt -> new RTFFileDataTables(fdt, segmentFactory, sorted, distinct))
                .peek(RTFFileDataTables::validate)
                .cache();
    }

    @Override
    public Collection<DateTime> getTimestamps() {
        final TLongList timestamps = body.map(RTFFileDataTables::getAllTimestamps).decodeOrThrow();
        final List<DateTime> result = new ArrayList<>(timestamps.size());
        timestamps
                .forEach(v -> {
                    result.add(new DateTime(v, DateTimeZone.UTC));
                    return true;
                });
        return result;
    }

    @Override
    public Set<GroupName> getGroupNames() {
        return body.map(RTFFileDataTables::getAllNames).decodeOrThrow();
    }

    @Override
    public Collection<DateTime> getGroupTimestamps(GroupName group) {
        return body.decodeOrThrow().getBlocks().stream()
                .flatMap(block -> getGroupTimestamps(block, group))
                .collect(Collectors.toList());
    }

    @Override
    public Set<MetricName> getMetricNames(GroupName group) {
        return body.map(tables -> tables.getGroupReaders(group)).decodeOrThrow().stream()
                .map(segmentReader -> {
                    return segmentReader
                            .map(groupTable -> groupTable.getMetricNames())
                            .decodeOrThrow();
                })
                .collect(HashSet<MetricName>::new, Collection::addAll, Collection::addAll);
    }

    @Override
    public Map<DateTime, MetricValue> getMetricValues(GroupName group, MetricName metric) {
        return body.decodeOrThrow().getBlocks().stream()
                .flatMap(block -> getMetricValues(block, group, metric))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Stream<Map.Entry<DateTime, MetricValue>> getMetricValues(RTFFileDataTablesBlock block, GroupName group, MetricName metric) {
        final long[] timestamps = block.getTimestamps();
        final SegmentReader<RTFGroupTable> groupTable = block.getTable().decodeOrThrow().getOrDefault(group.getPath(), emptyMap()).get(group);
        if (groupTable == null)
            return Stream.empty();

        final SegmentReader<RTFMetricTable> metricTable = groupTable.decodeOrThrow().getMetrics().get(metric);
        if (metricTable == null)
            return Stream.empty();

        final MetricValue[] metrics = metricTable.decodeOrThrow().getAll(0, timestamps.length);
        assert (timestamps.length == metrics.length);

        return IntStream.range(0, timestamps.length)
                .filter(idx -> metrics[idx] != null)
                .mapToObj(idx -> SimpleMapEntry.create(new DateTime(timestamps[idx], DateTimeZone.UTC), metrics[idx]));
    }

    private static Stream<DateTime> getGroupTimestamps(RTFFileDataTablesBlock block, GroupName group) {
        final long[] timestamps = block.getTimestamps();
        final SegmentReader<RTFGroupTable> groupTable = block.getTable().decodeOrThrow().getOrDefault(group.getPath(), emptyMap()).get(group);
        if (groupTable == null)
            return Stream.empty();

        return IntStream.range(0, timestamps.length)
                .filter(groupTable.decodeOrThrow()::contains)
                .mapToObj(idx -> new DateTime(timestamps[idx], DateTimeZone.UTC));
    }

    @Override
    public ColumnMajorTSData asColumnMajorTSData() {
        return this;
    }
}
