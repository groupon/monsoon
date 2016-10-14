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

import com.groupon.lex.metrics.history.TSData;
import com.groupon.lex.metrics.history.v2.Compression;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.Const;
import static com.groupon.lex.metrics.history.xdr.Const.MIME_HEADER_LEN;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrow;
import com.groupon.lex.metrics.history.xdr.support.DecodingException;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.reader.Crc32VerifyingFileReader;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelSegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SegmentReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Stream;
import lombok.Getter;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.joda.time.DateTime;

public class ReadonlyTableFile implements TSData {
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
    public Iterator<TimeSeriesCollection> iterator() {
        try {
            return body.decode().iterator();
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("decoding failed", ex);
        }
    }

    @Override
    public Spliterator<TimeSeriesCollection> spliterator() {
        try {
            return body.decode().spliterator();
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("decoding failed", ex);
        }
    }

    @Override
    public Stream<TimeSeriesCollection> streamReversed() {
        try {
            return body.decode().streamReversed();
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("decoding failed", ex);
        }
    }

    @Override
    public Stream<TimeSeriesCollection> stream() {
        try {
            return body.decode().stream();
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("decoding failed", ex);
        }
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        try {
            return body.decode().stream(begin);
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("decoding failed", ex);
        }
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        try {
            return body.decode().stream(begin, end);
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("decoding failed", ex);
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        try {
            return body.decode().size();
        } catch (IOException | OncRpcException ex) {
            throw new DecodingException("decoding failed", ex);
        }
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
        if ((hdr.flags & header_flags.DISTINCT) != header_flags.DISTINCT)
            throw new IllegalArgumentException("Bad TableFile: marked as containing duplicate timestamps");
        if ((hdr.flags & header_flags.SORTED) != header_flags.SORTED)
            throw new IllegalArgumentException("Bad TableFile: marked as unsorted");
        begin = FromXdr.timestamp(hdr.first);
        end = FromXdr.timestamp(hdr.last);

        segmentFactory = new FileChannelSegmentReader.Factory(file, compression);
        final FilePos bodyPos = FromXdr.filePos(hdr.fdt);

        body = segmentFactory.get(file_data_tables::new, bodyPos)
                .map(fdt -> new RTFFileDataTables(fdt, segmentFactory))
                .peek(RTFFileDataTables::validate)
                .cache();
    }
}
