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
import com.groupon.lex.metrics.history.xdr.support.reader.FileChannelReader;
import com.groupon.lex.metrics.history.xdr.support.reader.SizeVerifyingReader;
import com.groupon.lex.metrics.history.xdr.support.reader.XdrDecodingFileReader;
import com.groupon.lex.metrics.history.xdr.support.writer.CloseInhibitingWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;
import org.acplt.oncrpc.XdrEncodingStream;

public class MetricsTmpfile implements Closeable {
    private final FileChannel fd;
    private int count = 0;
    private static final Path TMPDIR = new File(System.getProperty("java.io.tmpdir")).toPath();
    private final FileChannelWriter writer;
    private XdrEncodingFileWriter xdr;

    public MetricsTmpfile() throws IOException {
        this(TMPDIR);
    }

    public MetricsTmpfile(Path dir) throws IOException {
        fd = FileUtil.createTempFile(dir, "monsoon_metrics", ".tmp");
        writer = new FileChannelWriter(fd, 0);
        xdr = new XdrEncodingFileWriter(new CloseInhibitingWriter(writer), ByteBuffer.allocateDirect(16384));

        try {
            xdr.beginEncoding();
        } catch (OncRpcException ex) {
            IOException toBeThrown = new IOException("serialization error (temporary storage)", ex);
            try {
                fd.close();
            } catch (IOException ex1) {
                toBeThrown.addSuppressed(ex1);
            }
            throw toBeThrown;
        }
    }

    public void add(long timestamp, MetricName name, MetricValue value, DictionaryForWrite dict) throws IOException {
        final SerializedForm sf = new SerializedForm(
                timestamp,
                dict.getPathTable().getOrCreate(name.getPath()),
                ToXdr.metricValue(value, dict.getStringTable()::getOrCreate));

        try {
            if (xdr == null) {
                XdrEncodingFileWriter newXdr = new XdrEncodingFileWriter(new CloseInhibitingWriter(writer), ByteBuffer.allocateDirect(16384));
                newXdr.beginEncoding();
                xdr = newXdr;
            }

            sf.xdrEncode(xdr);
        } catch (OncRpcException ex) {
            throw new IOException("serialization error (temporary storage)", ex);
        }

        ++count;
    }

    private long syncOffset() {
        try {
            xdr.endEncoding();
            xdr.close();
            xdr = null;
            return writer.getOffset();
        } catch (OncRpcException | IOException ex) {
            throw new RuntimeException("serialization error (temporary storage)", ex);
        }
    }

    public Stream<Tuple> stream(DictionaryDelta dict) throws IOException {
        return StreamSupport.stream(() -> new SpliteratorImpl(dict, syncOffset()), SpliteratorImpl.CHARACTERISTICS, false);
    }

    @Override
    public void close() throws IOException {
        this.fd.close();
    }

    @Value
    public static class Tuple {
        private final long timestamp;
        private final int metricPathRef;
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

    @RequiredArgsConstructor
    private class SpliteratorImpl implements Spliterator<Tuple> {
        public static final int CHARACTERISTICS =
                Spliterator.SIZED |
                Spliterator.SUBSIZED |
                Spliterator.IMMUTABLE |
                Spliterator.NONNULL;
        private final DictionaryDelta dict;
        private int index = 0;
        private final int count = MetricsTmpfile.this.count;
        private final XdrDecodingFileReader xdr;

        public SpliteratorImpl(DictionaryDelta dict, long endOffset) {
            this.dict = dict;
            this.xdr = new XdrDecodingFileReader(new SizeVerifyingReader(new FileChannelReader(fd, 0), endOffset), ByteBuffer.allocateDirect(65536));
            xdr.beginDecoding();
        }

        @Override
        public boolean tryAdvance(Consumer<? super Tuple> action) {
            try {
                if (index == count) return false;

                final SerializedForm sf = new SerializedForm();
                sf.xdrDecode(xdr);

                ++index;
                action.accept(new Tuple(
                        sf.getTimestamp(),
                        sf.getMetricNameRef(),
                        FromXdr.metricValue(sf.getMetricValue(), dict::getString)));
                if (index == count) {
                    xdr.endDecoding();
                    xdr.close();
                }
                return true;
            } catch (IOException | OncRpcException ex) {
                throw new RuntimeException("temporary file doesn't work properly");
            }
        }

        @Override
        public Spliterator<Tuple> trySplit() { return null; }

        @Override
        public long estimateSize() { return count; }

        @Override
        public int characteristics() {
            return CHARACTERISTICS;
        }
    }
}
