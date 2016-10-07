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
package com.groupon.lex.metrics.history.xdr.support.reader;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import static java.lang.Integer.min;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import lombok.NonNull;

public class GzipReader implements FileReader {
    private final GZIPInputStream gzip;

    public GzipReader(FileReader in) throws IOException {
        this.gzip = new GZIPInputStream(new GzAdapter(in));
    }

    @Override
    public int read(ByteBuffer data) throws IOException {
        final int rlen;
        if (data.hasArray()) {
            rlen = gzip.read(data.array(), data.arrayOffset() + data.position(), data.remaining());
            if (rlen > 0)
                data.position(data.position() + rlen);
        } else {
            byte tmp[] = new byte[data.remaining()];
            rlen = gzip.read(tmp);
            if (rlen > 0) data.put(tmp, 0, rlen);
        }

        if (rlen == -1)
            throw new EOFException("no more data (gzip)");
        return rlen;
    }

    @Override
    public void close() throws IOException {
        gzip.close();
    }

    @Override
    public ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    private static class GzAdapter extends InputStream {
        private final ByteBuffer buf;
        private final FileReader in;

        public GzAdapter(@NonNull FileReader in) {
            this.in = in;
            this.buf = in.allocateByteBuffer(8192);
        }

        @Override
        public int read() throws IOException {
            try {
                while (!buf.hasRemaining()) readMore();
                return (int)buf.get() & 0xff;
            } catch (EOFException ex) {
                return -1;
            }
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            try {
                int rlen = 0;
                while (len > 0) {
                    if (!buf.hasRemaining()) readMore();

                    final int bufGetLen = min(len, buf.remaining());
                    buf.get(b, off, bufGetLen);
                    off += bufGetLen;
                    len -= bufGetLen;
                    rlen += bufGetLen;
                }
                return rlen;
            } catch (EOFException ex) {
                return -1;
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        private void readMore() throws IOException {
            buf.compact();
            in.read(buf);
            buf.flip();
        }
    }
}