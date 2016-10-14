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
package com.groupon.lex.metrics.history.v2.list;

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.calculateDictionary;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.calculateTimeSeries;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.readAllTSDataHeaders;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.readTSDataHeader;
import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.Util.ALL_HDR_CRC_LEN;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.record;
import com.groupon.lex.metrics.history.v2.xdr.record_array;
import com.groupon.lex.metrics.history.v2.xdr.record_metric;
import com.groupon.lex.metrics.history.v2.xdr.record_metrics;
import com.groupon.lex.metrics.history.v2.xdr.record_tags;
import com.groupon.lex.metrics.history.v2.xdr.tsdata;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.Const;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelSegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter.Writer;
import com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;

/**
 *
 * @author ariane
 */
public final class ReadWriteState implements State {
    private static final Logger LOG = Logger.getLogger(ReadWriteState.class.getName());
    private final ReadWriteLock guard = new ReentrantReadWriteLock(true);  // Protects internal data structures.
    private final List<SegmentReader<TimeSeriesCollection>> tsdata;
    @Getter(AccessLevel.PRIVATE)
    private SegmentReader<DictionaryDelta> dictionary;
    @Getter
    private final GCCloseable<FileChannel> file;
    private final List<SegmentReader<ReadonlyTSDataHeader>> tsdataHeaders;
    private DictionaryForWrite writerDictionary;
    private tsfile_header hdr;
    private final Compression compression;

    public ReadWriteState(GCCloseable<FileChannel> file, tsfile_header hdr) throws IOException, OncRpcException {
        this.file = file;
        this.hdr = hdr;
        this.compression = Compression.fromFlags(this.hdr.flags);
        tsdataHeaders = readAllTSDataHeaders(file, FromXdr.filePos(hdr.fdt));
        dictionary = calculateDictionary(file, compression, tsdataHeaders)
                .cache();
        tsdata = calculateTimeSeries(file, compression, tsdataHeaders, SegmentReader.ofSupplier(this::getDictionary).flatMap(x -> x));

        writerDictionary = new DictionaryForWrite(dictionary.decode());
    }

    private <T> T doReadLocked(Supplier<T> fn) {
        Lock lock = guard.readLock();
        lock.lock();
        try {
            return fn.get();
        } finally {
            lock.unlock();
        }
    }

    public boolean isSorted() {
        return doReadLocked(() -> (hdr.flags & header_flags.SORTED) == header_flags.SORTED);
    }

    public boolean isDistinct() {
        return doReadLocked(() -> (hdr.flags & header_flags.DISTINCT) == header_flags.DISTINCT);
    }

    @Override
    public DateTime getBegin() {
        return doReadLocked(() -> FromXdr.timestamp(hdr.first));
    }

    @Override
    public DateTime getEnd() {
        return doReadLocked(() -> FromXdr.timestamp(hdr.last));
    }

    @Override
    public ObjectSequence<SegmentReader<TimeSeriesCollection>> sequence() {
        return doReadLocked(() -> {
            boolean sorted = (hdr.flags & header_flags.SORTED) == header_flags.SORTED;
            boolean distinct = (hdr.flags & header_flags.DISTINCT) == header_flags.DISTINCT;
            return new ForwardSequence(0, tsdata.size())
                    .map(tsdata::get, sorted, true, distinct);
        });
    }

    @Override
    public void add(TimeSeriesCollection tsc) {
        try {
            addRecords(singletonList(tsc));
        } catch (IOException | OncRpcException ex) {
            throw new RuntimeException("unable to add records", ex);
        }
    }

    @Override
    public void addAll(Collection<? extends TimeSeriesCollection> tsc) {
        try {
            addRecords(tsc);
        } catch (IOException | OncRpcException ex) {
            throw new RuntimeException("unable to add records", ex);
        }
    }

    private synchronized void addRecords(Collection<? extends TimeSeriesCollection> tscList) throws IOException, OncRpcException {
        LOG.log(Level.FINER, "adding {0} records", tscList.size());
        if (tscList.isEmpty())
            return;

        long recordOffset;
        final DictionaryForWrite newWriterDict;
        FilePos lastRecord;
        {
            final Lock lock = guard.readLock();
            lock.lock();
            try {
                recordOffset = hdr.file_size;
                lastRecord = FromXdr.filePos(hdr.fdt);
                newWriterDict = writerDictionary.clone();
                newWriterDict.reset();
            } finally {
                lock.unlock();
            }
        }
        LOG.log(Level.FINER, "newWriterDict initialized (strOffset={0}, pathOffset={1}, tagsOffset={2})",
                new Object[]{
                    newWriterDict.getStringTable().getOffset(),
                    newWriterDict.getPathTable().getOffset(),
                    newWriterDict.getTagsTable().getOffset()});
        final ByteBuffer useBuffer = (compression != Compression.NONE ? ByteBuffer.allocate(65536) : ByteBuffer.allocateDirect(65536));
        if (lastRecord.getOffset() == 0)
            lastRecord = null;  // Empty file.

        final List<FilePos> headers = new ArrayList<>(tscList.size());
        final List<EncodedTscHeaderForWrite> writeHeaders;
        try (FileChannelWriter fd = new FileChannelWriter(this.file.get(), recordOffset)) {
            {
                final Writer writer = new Writer(fd, compression, useBuffer, false);

                /* Write the contents of each collection. */
                writeHeaders = new ArrayList<>(tscList.size());
                for (TimeSeriesCollection tsc : tscList)
                    writeHeaders.add(writeTSC(writer, tsc, newWriterDict));

                /* First header owns reference to dictionary delta. */
                if (!newWriterDict.isEmpty())
                    writeHeaders.get(0).setNewWriterDict(Optional.of(writer.write(newWriterDict.encode())));
            }

            /* Write the headers. */
            for (EncodedTscHeaderForWrite header : writeHeaders) {
                header.setPreviousTscHeader(Optional.ofNullable(lastRecord));
                lastRecord = header.write(fd, useBuffer);
                headers.add(lastRecord);
            }

            recordOffset = fd.getOffset();  // New file end.
        }

        /* Prepare new dictionary for installation. */
        final DictionaryDelta newDictionary;
        if (newWriterDict.isEmpty()) {
            newDictionary = null;
            LOG.log(Level.FINER, "not installing new dictionary: delta is empty");
        } else {
            newDictionary = newWriterDict.asDictionaryDelta();
            writerDictionary = newWriterDict;
            LOG.log(Level.FINER, "installing new dictionary");
        }

        /*
         * Prepare updated file header.
         */
        final tsfile_header hdr = updateHeaderData(writeHeaders);

        /*
         * Update shared datastructures.
         */
        final Lock lock = guard.writeLock();
        lock.lock();
        try {
            /*
             * Write the header.
             *
             * Up to this point, we could abandon the write and the file_end mark
             * would pretend nothing was written.
             */
            hdr.file_size = recordOffset;
            hdr.fdt = ToXdr.filePos(lastRecord);
            writeHeader(hdr, useBuffer);
            this.hdr = hdr;

            /*
             * Update all data structures.
             */
            final List<SegmentReader<ReadonlyTSDataHeader>> newTsdataHeaders = headers.stream()
                    .map(offset -> readTSDataHeader(file, offset))
                    .collect(Collectors.toList());

            tsdataHeaders.addAll(newTsdataHeaders);
            if (newDictionary != null) {
                dictionary = calculateDictionary(file, compression, tsdataHeaders)
                        .cache(newDictionary);
            }
            tsdata.addAll(newTsdataHeaders.stream()
                    .map(roHdr -> {
                        return roHdr
                                .map(tsdHeader -> {
                                    final SegmentReader<DictionaryDelta> dictSegment = SegmentReader.ofSupplier(this::getDictionary)
                                            .flatMap(Function.identity());
                                    final SegmentReader<record_array> recordsSegment = tsdHeader.recordsDecoder(file, compression);
                                    final TimeSeriesCollection newTsc = new ListTSC(tsdHeader.getTimestamp(), recordsSegment, dictSegment, new FileChannelSegmentReader.Factory(file, compression));
                                    return newTsc;
                                })
                                .share();
                    })
                    .collect(Collectors.toList()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Update all flags/metadata in header for update.
     */
    private tsfile_header updateHeaderData(List<EncodedTscHeaderForWrite> headers) {
        // Make copy of header and work on that.
        final tsfile_header hdr = doReadLocked(() -> {
            tsfile_header copy = new tsfile_header();
            copy.first = this.hdr.first;
            copy.last = this.hdr.last;
            copy.file_size = this.hdr.file_size;
            copy.flags = this.hdr.flags;
            copy.reserved = this.hdr.reserved;
            copy.fdt = this.hdr.fdt;
            return copy;
        });

        if (isDistinct()) {
            boolean distinct = true;
            Stream<DateTime> tsStream = headers.stream().map(EncodedTscHeaderForWrite::getTimestamp);
            if (!headers.get(0).getTimestamp().isAfter(getEnd())) {
                try {
                    tsStream = Stream.concat(tsStream, sequence()
                            .reverse()
                            .map(segment -> {
                                try {
                                    return segment.decode().getTimestamp();
                                } catch (IOException | OncRpcException ex) {
                                    throw new RuntimeException("unable to decode", ex);
                                }
                            }, true, true, true)
                            .stream());
                } catch (Exception ex) {
                    LOG.log(Level.WARNING, "read error during new record write", ex);
                    distinct = false;
                }

                if (distinct) {
                    distinct = tsStream
                            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                            .values()
                            .stream()
                            .allMatch(count -> count == 1);
                }

                if (!distinct)
                    hdr.flags &= ~header_flags.DISTINCT;
            }
        }

        if (!tsdataHeaders.isEmpty() && !headers.get(0).getTimestamp().isAfter(FromXdr.timestamp(hdr.last)))
            hdr.flags &= ~header_flags.SORTED;
        if (tsdataHeaders.isEmpty() || headers.get(0).getTimestamp().isBefore(FromXdr.timestamp(hdr.first)))
            hdr.first = ToXdr.timestamp(headers.get(0).getTimestamp());
        if (tsdataHeaders.isEmpty() || headers.get(headers.size() - 1).getTimestamp().isAfter(FromXdr.timestamp(hdr.last)))
            hdr.last = ToXdr.timestamp(headers.get(headers.size() - 1).getTimestamp());

        return hdr;
    }

    /**
     * Write current header to file.
     */
    private void writeHeader(tsfile_header hdr, ByteBuffer useBuffer) throws OncRpcException, IOException {
        try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(file.get(), 0), ALL_HDR_CRC_LEN), 4), useBuffer)) {
            Const.writeMimeHeader(writer);
            hdr.xdrEncode(writer);
        }
    }

    private SegmentReader<TimeSeriesCollection> loadAtIndex(int index) {
        return doReadLocked(() -> tsdata.get(index));
    }

    private static EncodedTscHeaderForWrite writeTSC(Writer writer, TimeSeriesCollection tsc, DictionaryForWrite dict) throws IOException, OncRpcException {
        final List<record> recordList = new ArrayList<>();
        for (SimpleGroupPath path : tsc.getGroupPaths()) {
            record r = new record();
            r.path_ref = dict.getPathTable().getOrCreate(path.getPath());
            r.tags = writeTSCTags(writer, path, tsc, dict);
            recordList.add(r);
        }
        record_array ra = new record_array(recordList.toArray(new record[0]));

        final FilePos pos = writer.write(ra);
        return new EncodedTscHeaderForWrite(tsc.getTimestamp(), pos, dict);
    }

    private static record_tags[] writeTSCTags(Writer writer, SimpleGroupPath path, TimeSeriesCollection tsc, DictionaryForWrite dict) throws IOException, OncRpcException {
        final List<record_tags> recordList = new ArrayList<>();
        for (TimeSeriesValue tsv : tsc.getTSValue(path).stream().collect(Collectors.toList())) {
            record_tags rt = new record_tags();
            rt.tag_ref = dict.getTagsTable().getOrCreate(tsv.getTags());
            rt.pos = ToXdr.filePos(writeMetrics(writer, tsv.getMetrics(), dict));
            recordList.add(rt);
        }
        return recordList.toArray(new record_tags[0]);
    }

    private static FilePos writeMetrics(Writer writer, Map<MetricName, MetricValue> metrics, DictionaryForWrite dict) throws IOException, OncRpcException {
        record_metrics rma = new record_metrics(metrics.entrySet().stream()
                .map(metricEntry -> {
                    record_metric rm = new record_metric();
                    rm.path_ref = dict.getPathTable().getOrCreate(metricEntry.getKey().getPath());
                    rm.v = ToXdr.metricValue(metricEntry.getValue(), dict.getStringTable()::getOrCreate);
                    return rm;
                })
                .toArray(record_metric[]::new));
        return writer.write(rma);
    }

    private static class EncodedTscHeaderForWrite {
        @Getter
        @Setter
        private Optional<FilePos> previousTscHeader = Optional.empty();
        @Getter
        @Setter
        private Optional<FilePos> newWriterDict = Optional.empty();
        @Getter
        private final DateTime timestamp;
        private final FilePos encodedTsc;

        public EncodedTscHeaderForWrite(@NonNull DateTime ts, @NonNull FilePos encodedTsc, @NonNull DictionaryForWrite dict) {
            timestamp = ts;
            this.encodedTsc = encodedTsc;
        }

        public FilePos write(FileChannelWriter fd, ByteBuffer useBuffer) throws IOException, OncRpcException {
            final tsdata tscHdr = new tsdata();
            tscHdr.reserved = 0;
            tscHdr.dict = newWriterDict.map(ToXdr::filePos).orElse(null);
            tscHdr.previous = previousTscHeader.map(ToXdr::filePos).orElse(null);
            tscHdr.ts = ToXdr.timestamp(timestamp);
            tscHdr.records = ToXdr.filePos(encodedTsc);

            FilePos pos = new Writer(fd, Compression.NONE, useBuffer, false).write(tscHdr);
            LOG.log(Level.FINEST, "tsdata header written at {0}", pos);
            return pos;
        }
    }
}
