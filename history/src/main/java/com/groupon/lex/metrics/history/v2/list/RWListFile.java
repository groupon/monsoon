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

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import static com.groupon.lex.metrics.history.v2.xdr.Util.fixSequence;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.Const;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrow;
import static com.groupon.lex.metrics.history.xdr.Const.writeMimeHeader;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.reader.Crc32VerifyingFileReader;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter;
import static com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter.CRC_LEN;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import com.groupon.lex.metrics.lib.sequence.ObjectSequence;
import com.groupon.lex.metrics.lib.sequence.ReverseSequence;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class RWListFile implements TSData {
    private static final short FILE_VERSION = 2;  // Only file version that uses Table format.
    private final State state;

    public RWListFile(@NonNull GCCloseable<FileChannel> file, boolean forWrite) throws IOException, OncRpcException {
        final tsfile_header hdr;

        /* Nest readers to handle CRC verification of the header. */
        try (XdrDecodingFileReader reader = new XdrDecodingFileReader(new Crc32VerifyingFileReader(new FileChannelReader(file.get(), 0), MIME_HEADER_LEN + HDR_3_LEN, 0), MIME_HEADER_LEN + HDR_3_LEN)) {
            reader.beginDecoding();

            /* Check the mime header and version number first. */
            final int version = validateHeaderOrThrow(reader);
            if (Const.version_major(version) != FILE_VERSION)
                throw new IllegalArgumentException("ListFile is version " + FILE_VERSION + " only");

            /* Read the header; we don't actually process its information until
             * the CRC validation completes (it's in the close part of try-with-resources). */
            hdr = new tsfile_header(reader);

            reader.endDecoding();
        } // CRC is validated here.

        if (hdr.file_size > file.get().size())
            throw new IOException("file truncated");
        if ((hdr.flags & header_flags.KIND_MASK) != header_flags.KIND_LIST)
            throw new IllegalArgumentException("Not a file in list encoding");

        if (forWrite)
            state = new ReadWriteState(file, hdr);
        else
            state = new ReadOnlyState(file, hdr);
    }

    public static RWListFile newFile(@NonNull GCCloseable<FileChannel> file) throws IOException {
        return newFile(file, Compression.DEFAULT_APPEND);
    }

    public static RWListFile newFile(@NonNull GCCloseable<FileChannel> file, Compression compression) throws IOException {
        final tsfile_header hdr = new tsfile_header();
        hdr.last = hdr.first = ToXdr.timestamp(new DateTime(DateTimeZone.UTC));
        hdr.file_size = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
        hdr.flags = (header_flags.SORTED | header_flags.KIND_LIST | compression.compressionFlag);
        hdr.reserved = 0;
        hdr.fdt = ToXdr.filePos(new FilePos(0, 0));

        try {
            try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(file.get(), 0), MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN), 0))) {
                writer.beginEncoding();
                writeMimeHeader(writer, FILE_VERSION, (short) 0);
                hdr.xdrEncode(writer);
                writer.endEncoding();
            }

            return new RWListFile(file, true);
        } catch (OncRpcException ex) {
            throw new IllegalStateException("newly created file may never throw encoding errors", ex);
        }
    }

    @Override
    public boolean add(TimeSeriesCollection tsc) {
        state.add(tsc);
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends TimeSeriesCollection> tsc) {
        state.addAll(tsc);
        return !tsc.isEmpty();
    }

    @Override
    public short getMajor() {
        return FILE_VERSION;
    }

    @Override
    public short getMinor() {
        return 0;
    }

    @Override
    public Optional<GCCloseable<FileChannel>> getFileChannel() {
        return Optional.of(state.getFile());
    }

    @Override
    public boolean isEmpty() {
        return state.sequence().isEmpty();
    }

    @Override
    public int size() {
        final ObjectSequence<SegmentReader<TimeSeriesCollection>> seq = state.sequence();
        if (seq.isSorted() && seq.isDistinct())
            return seq.size();
        return fixSequence(state.decodedSequence()).size();
    }

    public Stream<TimeSeriesCollection> streamReverse() {
        return StreamSupport.stream(
                () -> fixSequence(state.decodedSequence()).reverse().spliterator(),
                ReverseSequence.SPLITERATOR_CHARACTERISTICS,
                false);
    }

    @Override
    public Stream<TimeSeriesCollection> stream() {
        return StreamSupport.stream(
                () -> fixSequence(state.decodedSequence()).spliterator(),
                ForwardSequence.SPLITERATOR_CHARACTERISTICS,
                false);
    }

    @Override
    public Spliterator<TimeSeriesCollection> spliterator() {
        return fixSequence(state.decodedSequence()).spliterator();
    }

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        return fixSequence(state.decodedSequence()).iterator();
    }

    @Override
    public boolean canAddSingleRecord() {
        return true;
    }

    @Override
    public boolean isOptimized() {
        return false;
    }

    @Override
    public long getFileSize() {
        try {
            return state.getFile().get().size();
        } catch (IOException ex) {
            throw new RuntimeException("unable to determine file size", ex);
        }
    }

    @Override
    public DateTime getBegin() {
        return state.getBegin();
    }

    @Override
    public DateTime getEnd() {
        return state.getEnd();
    }
}
