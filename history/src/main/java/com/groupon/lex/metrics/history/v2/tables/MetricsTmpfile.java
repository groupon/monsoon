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

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.history.v2.DictionaryForWrite;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.xdr.support.FileUtil;
import com.groupon.lex.metrics.history.xdr.support.reader.CloseInhibitingReader;
import com.groupon.lex.metrics.history.xdr.support.reader.Crc32VerifyingFileReader;
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SizeVerifyingReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.history.xdr.support.writer.CloseInhibitingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.Crc32AppendingFileWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.SizeVerifyingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;
import org.acplt.oncrpc.XdrEncodingStream;

public class MetricsTmpfile implements Closeable {
    private final FileChannel fd;
    private long offset = 0;
    private final ByteBuffer useBuffer = ByteBuffer.allocateDirect(128);
    private static final Path TMPDIR = new File(System.getProperty("java.io.tmpdir")).toPath();

    public MetricsTmpfile() throws IOException {
        this(TMPDIR);
    }

    public MetricsTmpfile(Path dir) throws IOException {
        fd = FileUtil.createTempFile(dir, "monsoon_metrics", ".tmp");
    }

    public long add(long timestamp, MetricName name, MetricValue value, DictionaryForWrite dict) throws IOException {
        final SerializedForm sf = new SerializedForm(
                timestamp,
                dict.getPathTable().getOrCreate(name.getPath()),
                ToXdr.metricValue(value, dict.getStringTable()::getOrCreate));
        final long initOffset = offset;
        final long newOffset;
        final long segmentLen;

        try (FileChannelWriter writer = new FileChannelWriter(fd, offset + 8)) {
            try (Crc32AppendingFileWriter crcWriter = new Crc32AppendingFileWriter(new CloseInhibitingWriter(writer), 4)) {
                try (XdrEncodingFileWriter xdr = new XdrEncodingFileWriter(new CloseInhibitingWriter(crcWriter), useBuffer)) {
                    xdr.beginEncoding();
                    sf.xdrEncode(xdr);
                    xdr.endEncoding();
                } catch (OncRpcException ex) {
                    throw new IOException("serialization error (temporary storage)", ex);
                }

                segmentLen = crcWriter.getWritten();
            }

            newOffset = writer.getOffset();
        }

        try (XdrEncodingFileWriter xdr = new XdrEncodingFileWriter(new SizeVerifyingWriter(new FileChannelWriter(fd, offset), 8), useBuffer)) {
            xdr.beginEncoding();
            xdr.xdrEncodeLong(segmentLen);
            xdr.endEncoding();
        } catch (OncRpcException ex) {
            throw new IOException("serialization error (temporary storage)", ex);
        }

        offset = newOffset;
        return initOffset;
    }

    public ArrayList<Tuple> loadAll(DictionaryDelta dict) throws IOException {
        final ArrayList<Tuple> tuples = new ArrayList<>();

        try (FileChannelReader fdReader = new FileChannelReader(fd, 0)) {
            try (SizeVerifyingReader reader = new SizeVerifyingReader(new CloseInhibitingReader(fdReader), offset)) {
                while (fdReader.getOffset() != offset) {
                    final SerializedForm sf = new SerializedForm();
                    final long segmentLen;

                    try (XdrDecodingFileReader xdr = new XdrDecodingFileReader(new SizeVerifyingReader(new CloseInhibitingReader(reader), 8), useBuffer)) {
                        xdr.beginDecoding();
                        segmentLen = xdr.xdrDecodeLong();
                        xdr.endDecoding();
                    } catch (OncRpcException ex) {
                        throw new IOException("deserialization error (temporary storage)", ex);
                    }

                    try (XdrDecodingFileReader xdr = new XdrDecodingFileReader(new Crc32VerifyingFileReader(new CloseInhibitingReader(reader), segmentLen, 4), useBuffer)) {
                        xdr.beginDecoding();
                        sf.xdrDecode(xdr);
                        xdr.endDecoding();
                    } catch (OncRpcException ex) {
                        throw new IOException("deserialization error (temporary storage)", ex);
                    }

                    tuples.add(new Tuple(
                            sf.getTimestamp(),
                            MetricName.valueOf(dict.getPath(sf.getMetricNameRef())),
                            FromXdr.metricValue(sf.getMetricValue(), dict::getString)));
                }
            }
        }

        tuples.trimToSize();
        return tuples;
    }

    @Override
    public void close() throws IOException {
        this.fd.close();
    }

    @Value
    public static class Tuple {
        private final long timestamp;
        private final MetricName metricName;
        private final MetricValue metricValue;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class SerializedForm implements XdrAble {
        private long timestamp;
        private int metricNameRef;
        private metric_value metricValue;

        @Override
        public void xdrEncode(XdrEncodingStream xdr) throws OncRpcException, IOException {
            xdr.xdrEncodeLong(timestamp);
            xdr.xdrEncodeInt(metricNameRef);
            metricValue.xdrEncode(xdr);
        }

        @Override
        public void xdrDecode(XdrDecodingStream xdr) throws OncRpcException, IOException {
            timestamp = xdr.xdrDecodeLong();
            metricNameRef = xdr.xdrDecodeInt();
            metricValue = new metric_value(xdr);
        }
    }
}
