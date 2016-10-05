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
package com.groupon.lex.metrics.history.xdr.support.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;
import lombok.NonNull;
import static java.lang.Math.min;

public class GzipWriter implements FileWriter {
    private final GZIPOutputStream gzip;

    public GzipWriter(@NonNull FileWriter out) throws IOException {
        gzip = new GZIPOutputStream(new GzAdapter(out));
    }

    @Override
    public int write(ByteBuffer data) throws IOException {
        if (data.hasArray()) {
            final int wlen = data.remaining();
            gzip.write(data.array(), data.arrayOffset() + data.position(), wlen);
            data.position(data.limit());
            return wlen;
        } else {
            int written = 0;
            byte buf[] = new byte[512];
            while (data.hasRemaining()) {
                final int buflen = min(data.remaining(), buf.length);
                data.get(buf, 0, buflen);
                gzip.write(buf, 0, buflen);
                data.position(data.position() + buflen);
                written += buflen;
            }
            return written;
        }
    }

    @Override
    public void close() throws IOException {
        gzip.close();
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    private static class GzAdapter extends OutputStream {
        private final ByteBuffer buf;
        private final FileWriter out;

        public GzAdapter(@NonNull FileWriter out) {
            this.out = out;
            buf = out.allocateByteBuffer(8192);
        }

        @Override
        public void write(int b) throws IOException {
            while (!buf.hasRemaining()) flushOut();
            buf.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            while (len > 0) {
                while (!buf.hasRemaining()) flushOut();

                final int wlen = Math.min(len, buf.remaining());
                buf.put(b, off, wlen);
                off += wlen;
                len -= wlen;
            }
        }

        private void flushOut() throws IOException {
            buf.flip();
            out.write(buf);
            buf.compact();
        }

        @Override
        public void close() throws IOException {
            buf.flip();
            while (buf.hasRemaining())
                out.write(buf);
            buf.compact();
            out.close();
        }
    }
}
