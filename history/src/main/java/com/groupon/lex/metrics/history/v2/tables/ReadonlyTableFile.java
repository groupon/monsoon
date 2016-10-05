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
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import com.groupon.lex.metrics.history.xdr.Const;
import static com.groupon.lex.metrics.history.xdr.Const.validateHeaderOrThrow;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.SegmentBufferSupplier;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import com.groupon.lex.metrics.lib.GCCloseable;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.stream.Stream;
import lombok.Getter;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;
import org.joda.time.DateTime;

public class ReadonlyTableFile implements TSData {
    private static final short FILE_VERSION = 3;  // Only file version that uses Table format.
    private final SegmentReader<RTFFileDataTables> body;
    @Getter
    private final DateTime begin, end;
    @Getter
    private final long fileSize;
    @Getter
    private final boolean gzipped;
    private final int version;
    private final SegmentReader.Factory<XdrAble> segmentFactory;

    @Override
    public boolean add(TimeSeriesCollection tsv) {
        throw new UnsupportedOperationException("add");
    }

    @Override
    public Iterator<TimeSeriesCollection> iterator() {
        return stream().iterator();
    }

    @Override
    public Stream<TimeSeriesCollection> stream() {
        try {
            return body.decode().stream();
        } catch (IOException | OncRpcException ex) {
            throw new RuntimeException("decoding failed", ex);
        }
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        try {
            return body.decode().stream(begin);
        } catch (IOException | OncRpcException ex) {
            throw new RuntimeException("decoding failed", ex);
        }
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        try {
            return body.decode().stream(begin, end);
        } catch (IOException | OncRpcException ex) {
            throw new RuntimeException("decoding failed", ex);
        }
    }

    @Override
    public short getMajor() { return Const.version_major(version); }
    @Override
    public short getMinor() { return Const.version_minor(version); }

    public ReadonlyTableFile(GCCloseable<FileChannel> file) throws IOException, OncRpcException {
        fileSize = file.get().size();

        XdrDecodingStream reader = new XdrBufferDecodingStream(new SegmentBufferSupplier(file, 0, fileSize));
        version = validateHeaderOrThrow(reader);
        if (Const.version_major(version) != FILE_VERSION)
            throw new IllegalArgumentException("TableFile is version 3 only");

        tsfile_header hdr = new tsfile_header(reader);
        if ((hdr.flags & header_flags.KIND_MASK) != header_flags.KIND_TABLES)
            throw new IllegalArgumentException("Not a file in table encoding");
        gzipped = ((hdr.flags & header_flags.GZIP) == header_flags.GZIP);

        if ((hdr.flags & header_flags.DUP_TS) == header_flags.DUP_TS)
            throw new IllegalArgumentException("Bad TableFile: marked as containing duplicate timestamps");
        if ((hdr.flags & header_flags.SORTED) != header_flags.SORTED)
            throw new IllegalArgumentException("Bad TableFile: marked as unsorted");

        segmentFactory = new FileChannelSegmentReader.Factory(file, gzipped);
        begin = FromXdr.timestamp(hdr.first);
        end = FromXdr.timestamp(hdr.last);
        final FilePos bodyPos = new FilePos(hdr.fdt);

        body = segmentFactory.get(file_data_tables::new, bodyPos)
                .map(fdt -> new RTFFileDataTables(fdt, hdr.first.value, segmentFactory))
                .peek(RTFFileDataTables::validate)
                .cache();
    }
}
