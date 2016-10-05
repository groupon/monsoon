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

import com.groupon.lex.metrics.history.v2.Dictionary;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
import com.groupon.lex.metrics.history.v2.xdr.tsfile_header;
import static com.groupon.lex.metrics.history.xdr.Const.writeMimeHeader;
import static com.groupon.lex.metrics.history.xdr.support.ByteCountingXdrEncodingStream.xdrSize;
import com.groupon.lex.metrics.history.xdr.support.FileChannelXdrEncodingStream;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.FilePosXdrEncodingStream;
import com.groupon.lex.metrics.history.xdr.support.writer.AbstractSegmentWriter;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.joda.time.DateTime;

/**
 * Stateful writer for table-based file.
 *
 * Table based files have to be written in one go, as they can't be written
 * before all data has been gathered.
 * @author ariane
 */
public class ToXdrTables {
    private final DateTime begin;
    private DateTime end;
    private final Dictionary dictionary = new Dictionary();
    private final Tables tables;

    public ToXdrTables(@NonNull DateTime begin, @NonNull DateTime end) {
        this.begin = begin;
        this.end = this.begin;
        tables = new Tables(begin, dictionary.getPathTable(), dictionary.getTagsTable(), dictionary.getStringTable());
    }

    public void add(TimeSeriesCollection tsdata) {
        final DateTime ts = tsdata.getTimestamp();
        if (ts.isAfter(end)) end = ts;
        tables.add(tsdata);
    }

    public void add(DateTime ts, Stream<TimeSeriesValue> tsdata) {
        if (ts.isAfter(end)) end = ts;
        tables.add(ts, tsdata);
    }

    public void add(DateTime ts, TimeSeriesValue tsv) {
        if (ts.isAfter(end)) end = ts;
        tables.add(ts, tsv);
    }

    public void write(@NonNull FileChannel out, boolean compress) throws OncRpcException, IOException {
        /* Write mime header */
        FilePosXdrEncodingStream mimeWriter = new FileChannelXdrEncodingStream(out, 0);
        mimeWriter.beginEncoding();
        writeMimeHeader(mimeWriter);
        mimeWriter.endEncoding();
        final long outPos = mimeWriter.getFilePos().getEnd();

        /** Space for header. */
        final long skip = xdrSize(encodeHeader(compress, new FilePos(0, 0)));

        /* Write required data for tables (fills in file positions). */
        final long tablesEnd = tables.write(out, outPos + skip, compress);
        /* Write tables. */
        final FilePos bodyPos = new AbstractSegmentWriter() {
            @Override
            public XdrAble encode() {
                return encodeBody();
            }
        }.write(out, tablesEnd, compress);

        /* Write header with (now known) body position. */
        FilePosXdrEncodingStream hdrWriter = new FileChannelXdrEncodingStream(out, outPos);
        hdrWriter.beginEncoding();
        encodeHeader(compress, bodyPos).xdrEncode(hdrWriter);
        hdrWriter.endEncoding();
        if (hdrWriter.getFilePos().getLen() != skip)
            throw new IllegalStateException("header changed size between encodings");
    }

    private tsfile_header encodeHeader(boolean compress, FilePos pos) {
        tsfile_header hdr = new tsfile_header();
        hdr.first = ToXdr.timestamp(begin);
        hdr.last = ToXdr.timestamp(end);
        hdr.flags = (compress ? header_flags.GZIP : 0) |
                header_flags.SORTED |
                header_flags.KIND_TABLES;
        hdr.reserved = 0;
        hdr.fdt = pos.encode();
        return hdr;
    }

    private file_data_tables encodeBody() {
        file_data_tables result = new file_data_tables();
        result.tables_data = tables.encode();
        result.dictionary = dictionary.encode();
        return result;
    }
}
