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

import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import static com.groupon.lex.metrics.history.v2.xdr.Util.HDR_3_LEN;
import com.groupon.lex.metrics.history.v2.xdr.file_data_tables;
import com.groupon.lex.metrics.history.v2.xdr.header_flags;
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
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.io.IOException;
import static java.lang.Long.max;
import static java.lang.Long.min;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Stateful writer for table-based file.
 *
 * Table based files have to be written in one go, as they can't be written
 * before all data has been gathered.
 * @author ariane
 */
public class ToXdrTables {
    private long begin, end;
    private final TLongSet timestamps = new TLongHashSet();
    private final DictionaryForWrite dictionary = new DictionaryForWrite();
    private final Tables tables;

    public ToXdrTables() {
        tables = new Tables(dictionary);
    }

    public void add(TimeSeriesCollection tsdata) {
        add(tsdata.getTimestamp(), tsdata.getTSValues());
    }

    public void add(DateTime ts, Collection<TimeSeriesValue> tsdata) {
        tables.add(updateTimestamps(ts), tsdata);
    }

    public void add(DateTime ts, TimeSeriesValue tsv) {
        tables.add(updateTimestamps(ts), tsv);
    }

    private long updateTimestamps(DateTime timestamp) {
        final long ts = timestamp.toDateTime(DateTimeZone.UTC).getMillis();
        if (timestamps.isEmpty()) {
            begin = end = ts;
        } else {
            begin = min(begin, ts);
            end = max(end, ts);
        }
        timestamps.add(ts);
        return ts;
    }

    public void write(@NonNull FileChannel out, boolean compress) throws OncRpcException, IOException {
        /** Space for headers. */
        final long hdrSpace = MIME_HEADER_LEN + HDR_3_LEN + CRC_LEN;
        final long hdrBegin = 0;
        final ByteBuffer buffer = (compress ? ByteBuffer.allocate(65536) : ByteBuffer.allocateDirect(65536));

        /** Create writer. */
        final FileChannelWriter fd = new FileChannelWriter(out, hdrBegin + hdrSpace);

        /** Write all tables. */
        final FilePos bodyPos = writeTables(fd, compress, buffer);

        /** Write the headers. */
        fd.setOffset(hdrBegin);
        try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new Crc32AppendingFileWriter(new SizeVerifyingWriter(fd, hdrSpace), 0), buffer)) {
            writer.beginEncoding();
            writeMimeHeader(writer);
            encodeHeader(compress, bodyPos, out.size()).xdrEncode(writer);
            writer.endEncoding();
        }
    }

    private FilePos writeTables(FileChannelWriter out, boolean compress, ByteBuffer useBuffer) throws IOException, OncRpcException {
        long[] timestamps = this.timestamps.toArray();
        Arrays.sort(timestamps);

        final Writer writer = new Writer(out, compress, useBuffer);
        tables.write(writer, timestamps);
        return writer.write(encodeBody(timestamps));
    }

    private tsfile_header encodeHeader(boolean compress, FilePos pos, long fileSize) {
        tsfile_header hdr = new tsfile_header();
        hdr.first = ToXdr.timestamp(begin);
        hdr.last = ToXdr.timestamp(end);
        hdr.flags = (compress ? header_flags.GZIP : 0) |
                header_flags.DISTINCT |
                header_flags.SORTED |
                header_flags.KIND_TABLES;
        hdr.reserved = 0;
        hdr.file_size = fileSize;
        hdr.fdt = ToXdr.filePos(pos);
        return hdr;
    }

    private file_data_tables encodeBody(long timestamps[]) {
        file_data_tables result = new file_data_tables();
        result.tsd = ToXdr.timestamp_delta(begin, timestamps);
        result.tables_data = tables.encode(timestamps);
        result.dictionary = dictionary.encode();
        return result;
    }
}
