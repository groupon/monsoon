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
import static java.lang.Math.min;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrEncodingStream;

public class XdrEncodingFileWriter extends XdrEncodingStream implements AutoCloseable {
    private final ByteBuffer buf;
    private final FileWriter out;
    private static final int MIN_BUFSIZ = 4;
    private static final int DEFAULT_BUFSIZ = 64 * 1024;

    public XdrEncodingFileWriter(FileWriter out) {
        this(out, DEFAULT_BUFSIZ);
    }

    public XdrEncodingFileWriter(@NonNull FileWriter out, int bufsiz) {
        if (bufsiz < MIN_BUFSIZ) bufsiz = MIN_BUFSIZ;
        this.out = out;
        buf = out.allocateByteBuffer(bufsiz);
        buf.order(ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void xdrEncodeInt(int i) throws OncRpcException, IOException {
        while (buf.remaining() < 4) flush();
        buf.putInt(i);
    }

    @Override
    public void xdrEncodeOpaque(byte[] bytes, int offset, int length) throws OncRpcException, IOException {
        if (length < 0) throw new IllegalArgumentException("negative length");
        int pad_len = length % 4 == 0 ? 0 : 4 - length % 4;

        while (length > 0) {
            while (!buf.hasRemaining()) flush();

            final int wlen = min(length, buf.remaining());
            buf.put(bytes, offset, wlen);
            offset += wlen;
            length -= wlen;
        }

        while (pad_len > 0) {
            while (!buf.hasRemaining()) flush();

            buf.put((byte)0);
            pad_len--;
        }
    }

    @Override
    public void beginEncoding(InetAddress receiverAddress, int receiverPort) throws OncRpcException, IOException {
        super.beginEncoding(receiverAddress, receiverPort);
    }
    public void beginEncoding() throws OncRpcException, IOException {
        beginEncoding(InetAddress.getLoopbackAddress(), 0);
    }

    @Override
    public void endEncoding() throws IOException, OncRpcException {
        super.endEncoding();
    }

    @Override
    public void close() throws OncRpcException, IOException {
        buf.flip();
        while (buf.hasRemaining())
            out.write(buf);
        buf.compact();
        out.close();
    }

    private void flush() throws IOException {
        buf.flip();
        out.write(buf);
        buf.compact();
    }
}
