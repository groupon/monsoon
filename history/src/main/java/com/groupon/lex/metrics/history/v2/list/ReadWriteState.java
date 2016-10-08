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

import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.Util.ALL_HDR_CRC_LEN;
import static com.groupon.lex.metrics.history.v2.xdr.Util.TSDATA_HDR_LEN;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.calculateDictionary;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.calculateTimeSeries;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.readAllTSDataHeaders;
import static com.groupon.lex.metrics.history.v2.list.ReadOnlyState.readTSDataHeader;
import com.groupon.lex.metrics.history.v2.xdr.record_array;
import com.groupon.lex.metrics.history.v2.xdr.tsdata;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.Const;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.ForwardSequence;
import com.groupon.lex.metrics.history.xdr.support.ObjectSequence;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter.Writer;
import com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter;
import static com.groupon.lex.metrics.history.xdr.support.writer.Crc32Writer.CRC_LEN;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;
import static java.util.Collections.singletonList;
import java.util.List;
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

    public ReadWriteState(GCCloseable<FileChannel> file, tsfile_header hdr) throws IOException, OncRpcException {
        this.file = file;
        this.hdr = hdr;
        tsdataHeaders = readAllTSDataHeaders(file, hdr.file_size);
        dictionary = calculateDictionary(file, isGzipped(), tsdataHeaders)
                .cache();
        tsdata = calculateTimeSeries(file, isGzipped(), tsdataHeaders, SegmentReader.ofSupplier(this::getDictionary).flatMap(x -> x));

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
    public boolean isGzipped() {
        return doReadLocked(() -> (hdr.flags & header_flags.GZIP) == header_flags.GZIP);
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
        if (tscList.isEmpty()) return;

        final boolean compressed;
        long recordOffset;
        final DictionaryForWrite newWriterDict;
        {
            final Lock lock = guard.readLock();
            lock.lock();
            try {
                compressed = (hdr.flags & header_flags.GZIP) == header_flags.GZIP;
                recordOffset = hdr.file_size;
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

        /* Encode all headers (which fills the newWriterDict in the process. */
        final List<EncodedTscHeaderForWrite> headers = tscList.stream()
                .sorted()
                .map(tsc -> new EncodedTscHeaderForWrite(tsc, newWriterDict))
                .collect(Collectors.toList());
        LOG.log(Level.FINER, "encoded {0} headers", headers);
        /* First header will write dictionary delta. */
        if (!newWriterDict.isEmpty())
            headers.get(0).setNewWriterDict(newWriterDict);

        /* Write all headers (fills in hdr.recordOffset from argument). */
        for (EncodedTscHeaderForWrite tscHdr : headers)
            recordOffset = tscHdr.write(file.get(), recordOffset, compressed);

        /* Prepare new dictionary for installation. */
        final DictionaryDelta newDictionary;
        if (newWriterDict.isEmpty()) {
            newDictionary = null;
            LOG.log(Level.FINER, "not installing new dictionary: delta is empty");
        } else {
            newDictionary = writerDictionary.asDictionaryDelta();
            writerDictionary = newWriterDict;
            LOG.log(Level.FINER, "installing new dictionary");
        }

        /*
         * Prepare updated file header.
         */

        final tsfile_header hdr = updateHeaderData(headers);

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
            writeHeader(hdr);
            this.hdr = hdr;

            /*
             * Update all data structures.
             */

            final List<SegmentReader<ReadonlyTSDataHeader>> newTsdataHeaders = headers.stream()
                    .mapToLong(EncodedTscHeaderForWrite::getRecordOffset)
                    .mapToObj(offset -> readTSDataHeader(file, offset))
                    .collect(Collectors.toList());

            tsdataHeaders.addAll(newTsdataHeaders);
            if (newDictionary != null) {
                dictionary = calculateDictionary(file, compressed, tsdataHeaders)
                        .cache(newDictionary);
            }
            tsdata.addAll(newTsdataHeaders.stream()
                    .map(roHdr -> {
                        return roHdr
                                .map(tsdHeader -> {
                                    final SegmentReader<DictionaryDelta> dictSegment = SegmentReader.ofSupplier(this::getDictionary)
                                            .flatMap(Function.identity());
                                    final SegmentReader<record_array> recordsSegment = tsdHeader.recordsDecoder(file, compressed);
                                    final TimeSeriesCollection newTsc = new ListTSC(tsdHeader.getTimestamp(), recordsSegment, dictSegment);
                                    return newTsc;
                                })
                                .share();
                    })
                    .collect(Collectors.toList()));
        } finally {
            lock.unlock();
        }
    }

    /** Update all flags/metadata in header for update. */
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

                if (!distinct) hdr.flags &= ~header_flags.DISTINCT;
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

    /** Write current header to file. */
    private void writeHeader(tsfile_header hdr) throws OncRpcException, IOException {
        try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(file.get(), 0), ALL_HDR_CRC_LEN), 4), ALL_HDR_CRC_LEN)) {
            Const.writeMimeHeader(writer);
            hdr.xdrEncode(writer);
        }
    }

    private SegmentReader<TimeSeriesCollection> loadAtIndex(int index) {
        return doReadLocked(() -> tsdata.get(index));
    }

    private static class EncodedTscHeaderForWrite {
        private static final int HEADER_BYTES = TSDATA_HDR_LEN + CRC_LEN;
        @Getter
        @Setter
        private DictionaryForWrite newWriterDict;  // May be null.
        @Getter
        private final DateTime timestamp;
        private final record_array encodedTsc;
        @Getter
        private long recordOffset = -1;

        public EncodedTscHeaderForWrite(TimeSeriesCollection tsc, DictionaryForWrite dict) {
            newWriterDict = null;
            timestamp = tsc.getTimestamp();
            encodedTsc = ToXdr.timeSeriesCollection(tsc, dict);
        }

        public long write(FileChannel file, long recordOffset, boolean compressed) throws IOException, OncRpcException {
            this.recordOffset = recordOffset;
            final long newFileEnd;
            final tsdata tscHdr = new tsdata();
            tscHdr.reserved = 0;
            tscHdr.ts = ToXdr.timestamp(timestamp);

            final long recordEnd = recordOffset + HEADER_BYTES;
            try (FileChannelWriter fileWriter = new FileChannelWriter(file, recordEnd)) {
                LOG.log(Level.FINEST, "file offset {0}", fileWriter.getOffset());
                final Writer writer = new Writer(fileWriter, compressed);
                if (newWriterDict == null) {
                    LOG.log(Level.FINEST, "dictionary absent -> not writing delta");
                    tscHdr.dd_len = 0;
                } else {
                    FilePos pos = writer.write(newWriterDict.encode());
                    tscHdr.dd_len = pos.getLen();
                    LOG.log(Level.FINEST, "dictionary present, encoded at position {0}", pos);
                }

                {
                    LOG.log(Level.FINEST, "file offset {0}", fileWriter.getOffset());
                    FilePos pos = writer.write(encodedTsc);
                    tscHdr.r_len = pos.getLen();
                    LOG.log(Level.FINEST, "records encoded at position {0}", pos);
                }

                newFileEnd = fileWriter.getOffset();
                LOG.log(Level.FINEST, "payload ends at {0}", newFileEnd);
            }

            try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(file, recordOffset), HEADER_BYTES), 4), TSDATA_HDR_LEN)) {
                writer.beginEncoding();
                tscHdr.xdrEncode(writer);
                writer.endEncoding();
                LOG.log(Level.FINEST, "tsdata header written at {0}", recordOffset);
            }

            return newFileEnd;
        }
    }
}
