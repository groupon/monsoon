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
package com.groupon.lex.metrics.history.xdr.support;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPOutputStream;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;

/**
 *
 * @author ariane
 */
public class GzipXdrEncodingStream extends FilePosXdrEncodingStream {
    private final FileChannel out;
    private final long initPos;
    private long outPos;
    private final ByteBuffer gzIn;
    private final ByteBuffer gzOut;
    private final GZIPOutputStream gzip;

    public GzipXdrEncodingStream(@NonNull FileChannel out, long outPos) throws IOException {
        this.out = out;
        this.initPos = outPos;
        this.outPos = this.initPos;
        gzIn = ByteBuffer.allocate(1024);
        gzOut = ByteBuffer.allocateDirect(8192);
        gzIn.order(ByteOrder.BIG_ENDIAN);
        gzip = new GZIPOutputStream(new GzAdapter());
    }

    public FilePos getFilePos() {
        return new FilePos(initPos, outPos - initPos);
    }

    @Override
    public void xdrEncodeInt(int i) throws OncRpcException, IOException {
        while (gzIn.remaining() < 4) flushIn();

        gzIn.putInt(i);
    }

    @Override
    public void xdrEncodeOpaque(byte[] bytes, int offset, int length) throws OncRpcException, IOException {
        if (length < 0) throw new IllegalArgumentException("negative length");
        int pad_len = length % 4 == 0 ? 0 : 4 - length % 4;

        while (length > 0) {
            if (!gzIn.hasRemaining()) {
                flushIn();
            }
            final int wlen = Math.min(length, gzIn.remaining());
            gzIn.put(bytes, offset, wlen);
            offset += wlen;
            length -= wlen;
        }

        while (pad_len > 0) {
            while (!gzIn.hasRemaining()) flushIn();

            gzIn.put((byte) 0);
            pad_len--;
        }
    }

    @Override
    public void endEncoding() throws IOException {
        gzIn.flip();
        gzip.write(gzIn.array(), gzIn.arrayOffset() + gzIn.position(), gzIn.remaining());
        gzip.close();
    }

    private void flushIn() throws IOException {
        gzIn.flip();
        gzip.write(gzIn.array(), gzIn.arrayOffset() + gzIn.position(), gzIn.remaining());
        gzIn.position(gzIn.position() + gzIn.remaining());
        gzIn.compact();
    }

    private void flushOut() throws IOException {
        gzOut.flip();
        outPos += out.write(gzOut, outPos);
        gzOut.compact();
    }

    private class GzAdapter extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            while (!gzOut.hasRemaining()) flushOut();

            gzOut.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            while (len > 0) {
                while (!gzOut.hasRemaining()) flushOut();

                final int wlen = Math.min(len, gzOut.remaining());
                gzOut.put(b, off, wlen);
                off += wlen;
                len -= wlen;
            }
        }

        @Override
        public void close() throws IOException {
            gzOut.flip();
            while (gzOut.hasRemaining())
                outPos += out.write(gzOut, outPos);
            gzOut.compact();
        }
    }
}
